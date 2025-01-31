import logging
import threading
import time
from enum import IntEnum
import traceback
import json

import asyncio
import httpx
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


class NodeInfo(BaseModel):
    nodeId: str
    url: str
    role: ServerRole
    status: int = 1


class Partition(BaseModel):
    partitionId: int
    nodeId: str


class PartitionConfig(BaseModel):
    partitionKey: str
    numberOfPartitions: int
    partitions: List[Partition]


class NodePartitionInfo:
    def __init__(self):
        self.partitionId: int = 0
        self.nodes: List[NodeInfo] = []

class PartitionConfigWrapper:
    def __init__(self):
        self.partitionKey: str = ""
        self.numberOfPartitions: int = 0
        self.nodesInfo: Dict[int, NodePartitionInfo] = {}

class ProxyServer:
    def __init__(self, master_host: str, master_port: int, instance_id: str):
        self.app = FastAPI()
        self.master_host = master_host
        self.master_port = master_port
        self.instance_id = instance_id

        # 节点配置相关
        self.nodes_buffers = [[], []]
        self.active_index = 0
        self.next_node_index = 0
        self.nodes_lock = threading.Lock()

        self.running = True

        # 分区相关
        self.partition_buffers = [PartitionConfigWrapper(), PartitionConfigWrapper()]
        self.active_partition_index = 0
        self.partition_lock = threading.Lock()
        
        self.setup_routes()
        # 节点信息更新
        self.start_update_thread()
        # 分区信息更新
        self.start_partition_update_thread()

        # 添加读写路径定义
        self.write_paths = {"/upsert"}
        self.read_paths = {"/search"}

    @property
    def active_nodes(self) -> List[NodeInfo]:
        """获取当前活动的节点列表"""
        return self.nodes_buffers[self.active_index]

    def get_target_node(self, path: str, force_master: bool = False) -> NodeInfo:
        """根据请求路径和参数选择目标节点"""
        nodes = self.active_nodes
        if not nodes:
            raise HTTPException(status_code=503, detail="No available nodes")

        # 写请求或强制主节点请求需要路由到主节点
        if force_master or path in self.write_paths:
            master_nodes = [node for node in nodes if node.role == ServerRole.MASTER]
            if not master_nodes:
                raise HTTPException(status_code=503, detail="No master node available")
            logger.info(f"Routing {'write' if path in self.write_paths else 'forced'} request to master node")
            return master_nodes[0]
        else:
            # 读请求 - 轮询所有节点
            node = nodes[self.next_node_index % len(nodes)]
            self.next_node_index += 1
            logger.info(f"Routing read request to node: {node.nodeId}")
            return node


    def setup_routes(self):
        @self.app.get("/topology")
        def get_topology():
            """返回当前系统拓扑信息"""
            nodes = self.active_nodes
            return {
                "masterServer": self.master_host,
                "masterServerPort": self.master_port,
                "instanceId": self.instance_id,
                "nodes": [
                    {
                        "nodeId": node.nodeId,
                        "url": node.url,
                        "role": int(node.role)  # 确保role以整数形式返回
                    } for node in nodes
                ]
            }

        @self.app.post("/upsert")
        async def upsert(request: Request):
            return await self.handle_partitioned_request(request, "/upsert")

        @self.app.post("/search")
        async def search(request: Request):
            return await self.handle_partitioned_request(request, "/search")

        @self.app.api_route("/{path:path}", methods=["GET", "POST"])
        async def forward_request(path: str, request: Request):
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

    
    async def handle_partitioned_request(self, request: Request, path: str):
        """处理需要分区路由的请求"""
        try:
            body = await request.json()
            partition_key_value = self.extract_partition_key(body)
            
            if partition_key_value is None:
                # 如果没有分区键，则广播到所有分区
                if path == "/search":
                    return await self.broadcast_search_request(request)
                else:
                    raise HTTPException(status_code=400, detail="Missing partition key")

            partition_id = self.calculate_partition_id(partition_key_value)
            target_node = self.select_partition_node(partition_id, path)
            
            # 转发请求到目标节点
            target_url = f"{target_node.url}{path}"
            async with httpx.AsyncClient() as client:
                response = await client.post(target_url, json=body)
                return response.json()

        except Exception as e:
            logger.error(f"Error handling partitioned request: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def broadcast_search_request(self, request: Request):
        """广播搜索请求到所有分区"""
        try:
            body = await request.json()
            k = body.get("k")
            if not isinstance(k, int):
                raise HTTPException(status_code=400, detail="Invalid or missing 'k' parameter")

            # 获取当前活动的分区配置
            partition_config = self.partition_buffers[self.active_partition_index]
            tasks = []

            # 为每个分区创建异步请求任务
            for partition_id in partition_config.nodesInfo:
                node = self.select_partition_node(partition_id, "/search")
                task = self.send_search_request(node.url, body)
                tasks.append(task)

            # 等待所有请求完成
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            
            
            # 合并结果
            all_results = []
            for response in responses:
                if isinstance(response, dict) and response.get("retcode") == 0:
                    vectors = response.get("vectors", [])
                    distances = response.get("distances", [])
                    all_results.extend(zip(distances, vectors))
                

            # 排序并限制结果数量
            all_results.sort(key=lambda x: x[0])  # 按距离排序
            all_results = all_results[:k]
            return {
                "retCode": 0,
                "vectors": [r[1] for r in all_results],
                "distances": [r[0] for r in all_results]
            }

        except Exception as e:
            logger.error(f"Error in broadcast search: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def send_search_request(self, node_url: str, body: dict) -> dict:
        """发送搜索请求到指定节点"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{node_url}/search", json=body)
                return response.json()
        except Exception as e:
            logger.error(f"Error sending search request to {node_url}: {str(e)}")
            return {"retCode": 1, "msg": str(e)}
        
    def extract_partition_key(self, body: dict) -> Optional[str]:
        """从请求体中提取分区键值"""
        partition_config = self.partition_buffers[self.active_partition_index]
        key = partition_config.partitionKey
        if not key or key not in body:
            return None

        value = body[key]
        return str(value) if isinstance(value, (str, int)) else None

    def calculate_partition_id(self, key_value: str) -> int:
        """计算分区 ID"""
        partition_config = self.partition_buffers[self.active_partition_index]
        hash_value = hash(key_value)
        return abs(hash_value) % partition_config.numberOfPartitions

    def select_partition_node(self, partition_id: int, path: str) -> NodeInfo:
        """选择分区节点"""
        partition_config = self.partition_buffers[self.active_partition_index]
        partition_info = partition_config.nodesInfo.get(partition_id)
        
        if not partition_info or not partition_info.nodes:
            raise HTTPException(status_code=503, detail=f"No nodes available for partition {partition_id}")

        # 写请求选择主节点
        if path in self.write_paths:
            master_nodes = [n for n in partition_info.nodes if n.role == ServerRole.MASTER]
            if not master_nodes:
                raise HTTPException(status_code=503, detail="No master node available")
            return master_nodes[0]
        
        # 读请求轮询选择节点
        node_index = self.next_node_index % len(partition_info.nodes)
        self.next_node_index += 1
        return partition_info.nodes[node_index]

    def fetch_and_update_nodes(self):
        """获取并更新节点信息，使用双缓冲区"""
        try:
            url = f"http://{self.master_host}:{self.master_port}/getInstance"
            params = {"instanceId": self.instance_id}
            
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if data["retCode"] != 0:
                logger.error(f"Error from Master Server: {data['msg']}")
                return

            inactive_index = 1 - self.active_index
            new_nodes = []
            
            # 只添加状态正常的节点
            for node_data in data["data"]["nodes"]:
                if node_data.get("status", 0) == 1:  # 只添加状态为1的节点
                    try:
                        node = NodeInfo(
                            nodeId=node_data["nodeId"],
                            url=node_data["url"],
                            role=ServerRole(node_data["role"])
                        )
                        new_nodes.append(node)
                    except Exception as e:
                        logger.warning(f"Failed to parse node data: {str(e)}")
                else:
                    logger.info(f"Skipping inactive node: {node_data.get('nodeId', 'unknown')}")

            self.nodes_buffers[inactive_index] = new_nodes            
            self.active_index = inactive_index
            logger.info(f"Nodes updated successfully, active nodes: {len(new_nodes)}")

        except Exception as e:
            logger.error(f"Error fetching nodes: {str(e)}\n{traceback.format_exc()}")

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


    def fetch_and_update_partition_config(self):
        """获取并更新分区配置"""
        try:
            url = f"http://{self.master_host}:{self.master_port}/getPartitionConfig"
            params = {"instanceId": self.instance_id}
            
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if data["retCode"] != 0:
                logger.error(f"Error fetching partition config: {data['msg']}")
                return

            inactive_index = 1 - self.active_partition_index
            new_config = PartitionConfigWrapper()
            
            # 更新分区配置
            new_config.partitionKey = data["data"]["partitionKey"]
            new_config.numberOfPartitions = data["data"]["numberOfPartitions"]
            
            # 更新分区节点信息
            for partition in data["data"]["partitions"]:
                partition_id = partition["partitionId"]
                node_id = partition["nodeId"]
                
                if partition_id not in new_config.nodesInfo:
                    new_config.nodesInfo[partition_id] = NodePartitionInfo()
                    new_config.nodesInfo[partition_id].partitionId = partition_id

                # 从活动节点列表中查找完整的节点信息
                for node in self.active_nodes:
                    if node.nodeId == node_id:
                        new_config.nodesInfo[partition_id].nodes.append(node)
                        break

            # 切换配置
            self.partition_buffers[inactive_index] = new_config
            self.active_partition_index = inactive_index
            
            logger.info("Partition configuration updated successfully")

        except Exception as e:
            logger.error(f"Error updating partition config: {str(e)}")

    def start_partition_update_thread(self):
        """启动分区配置更新线程"""
        def update_loop():
            while self.running:
                try:
                    self.fetch_and_update_partition_config()
                    time.sleep(300)
                except Exception as e:
                    logger.error(f"Error in partition update loop: {str(e)}")
                    time.sleep(5)

        thread = threading.Thread(target=update_loop)
        thread.daemon = True
        thread.start()

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