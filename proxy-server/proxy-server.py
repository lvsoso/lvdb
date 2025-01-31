import logging
import threading
import time
from enum import IntEnum
import traceback
import json

import requests
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ServerRole(IntEnum):
    MASTER = 0
    SLAVE = 1
    UNKNOWN = 2

class NodeInfo(BaseModel):
    nodeId: str
    url: str
    role: ServerRole

class ProxyServer:
    def __init__(self, master_host: str, master_port: int, instance_id: str):
        self.app = FastAPI()
        self.master_host = master_host
        self.master_port = master_port
        self.instance_id = instance_id
        self.nodes: List[NodeInfo] = []
        self.next_node_index = 0
        self.nodes_lock = threading.Lock()
        self.running = True
        
        self.setup_routes()
        self.start_update_thread()

        # 添加读写路径定义
        self.write_paths = {"/upsert"}
        self.read_paths = {"/search"}

    def get_target_node(self, path: str, force_master: bool = False) -> NodeInfo:
        """根据请求路径和参数选择目标节点"""
        with self.nodes_lock:
            if not self.nodes:
                raise HTTPException(status_code=503, detail="No available nodes")

            # 写请求或强制主节点请求需要路由到主节点
            if force_master or path in self.write_paths:
                master_nodes = [node for node in self.nodes if node.role == ServerRole.MASTER]
                if not master_nodes:
                    raise HTTPException(status_code=503, detail="No master node available")
                logger.info(f"Routing {'write' if path in self.write_paths else 'forced'} request to master node")
                return master_nodes[0]
            else:
                # 读请求 - 轮询所有节点
                node = self.nodes[self.next_node_index % len(self.nodes)]
                self.next_node_index += 1
                logger.info(f"Routing read request to node: {node.nodeId}")
                return node

    def setup_routes(self):
        @self.app.get("/topology")
        def get_topology():
            return {
                "masterServer": self.master_host,
                "masterServerPort": self.master_port,
                "instanceId": self.instance_id,
                "nodes": [
                    {
                        "nodeId": node.nodeId,
                        "url": node.url,
                        "role": node.role
                    } for node in self.nodes
                ]
            }

        @self.app.api_route("/{path:path}", methods=["GET", "POST"])
        async def forward_request(path: str, request: Request):
            if not self.nodes:
                raise HTTPException(status_code=503, detail="No available nodes")

            try:
                force_master = request.query_params.get("forceMaster", "").lower() == "true"
                node = self.get_target_node(path, force_master)
                
                target_url = f"{node.url}/{path}"
                method = request.method
                body = await request.body()
                params = dict(request.query_params)
                params.pop("forceMaster", None)
                
                # 记录转发信息
                logger.info(f"Forwarding {method} request to {node.role.name} node {node.nodeId} "
                        f"(URL: {target_url}, forceMaster={force_master})")
                
                # 发送请求
                response = requests.request(
                    method=method,
                    url=target_url,
                    params=params,
                    json=json.loads(body.decode('utf-8')) if body else None,
                    timeout=30
                )
                
                return response.json()

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error forwarding request: {str(e)}\n{traceback.format_exc()}")
                raise HTTPException(status_code=500, detail="Internal Server Error")

    def fetch_and_update_nodes(self):
        try:
            url = f"http://{self.master_host}:{self.master_port}/getInstance"
            params = {"instanceId": self.instance_id}
            
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if data["retCode"] != 0:
                logger.error(f"Error from Master Server: {data['msg']}")
                return

            new_nodes = []
            for node_data in data["data"]["nodes"]:
                node = NodeInfo(
                    nodeId=node_data["nodeId"],
                    url=node_data["url"],
                    role=node_data["role"]
                )
                new_nodes.append(node)

            with self.nodes_lock:
                self.nodes = new_nodes
            logger.info("Nodes updated successfully")

        except Exception as e:
            logger.error(f"Error fetching nodes: {str(e)}")

    def update_nodes_loop(self):
        while self.running:
            try:
                self.fetch_and_update_nodes()
                time.sleep(30)
            except Exception as e:
                logger.error(f"Error in update loop: {str(e)}")
                time.sleep(5)

    def start_update_thread(self):
        self.update_thread = threading.Thread(target=self.update_nodes_loop)
        self.update_thread.daemon = True
        self.update_thread.start()

    def run(self, host: str = "0.0.0.0", port: int = 80):
        self.fetch_and_update_nodes()
        uvicorn.run(self.app, host=host, port=port)

    def cleanup(self):
        self.running = False
        if hasattr(self, 'update_thread'):
            self.update_thread.join(timeout=1)

if __name__ == "__main__":
    master_host = "127.0.0.1"
    master_port = 8100
    instance_id = "instance1"
    proxy_port = 9090

    logger.info("Starting ProxyServer...")
    proxy = ProxyServer(master_host, master_port, instance_id)
    try:
        proxy.run(port=proxy_port)
    finally:
        proxy.cleanup()