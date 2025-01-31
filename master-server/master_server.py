import logging
from enum import IntEnum
from collections import defaultdict
import threading
import time

from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import etcd3
import uvicorn


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ServerRole(IntEnum):
    MASTER = 0
    SLAVE = 1
    UNKNOWN = 2


class ServerInfo(BaseModel):
    url: str
    role: ServerRole

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ServerInfo':
        return cls(
            url=data["url"],
            role=ServerRole(data["role"])
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "role": int(self.role)
        }

class NodeRequest(BaseModel):
    instanceId: str
    nodeId: str
    url: str
    role: ServerRole
    status: int

class ResponseModel(BaseModel):
    retCode: int
    msg: str
    data: Optional[Dict[str, Any]] = None

class MasterServer:
    def __init__(self, etcd_endpoints: str):
        self.app = FastAPI()
        self.etcd_client = etcd3.client(host=etcd_endpoints.split(':')[0],
                                      port=int(etcd_endpoints.split(':')[1]))

        # 记录节点的错误次数
        self.node_error_counts = defaultdict(int)
        self.running = True

        self.setup_routes()
        self.start_node_update_timer()

    def start_node_update_timer(self):
        """启动节点状态更新定时器"""
        def update_loop():
            while self.running:
                try:
                    self.update_node_states()
                    time.sleep(10)
                except Exception as e:
                    logger.error(f"Error in update loop: {str(e)}")
                    time.sleep(5)

        self.update_thread = threading.Thread(target=update_loop, daemon=True)
        self.update_thread.start()
    
    def update_node_states(self):
        """更新所有节点的状态"""
        logger.info("Fetching nodes list from etcd")
        
        def process_node(node_key: str, node_info: dict) -> None:
            """处理单个节点的状态更新"""
            node_url = node_info.get("url")
            if not node_url:
                logger.warning(f"Node {node_key} has no URL")
                return

            get_node_url = f"{node_url}/admin/getNode"
            logger.debug(f"Sending request to {get_node_url}")
            
            try:
                response = requests.get(get_node_url, timeout=5)
                if response.status_code != 200:
                    raise Exception(f"Bad response: {response.status_code}")
                    
                # 重置错误计数并更新状态
                self.node_error_counts[node_key] = 0
                needs_update = self._update_node_status(node_info, response.json())
                
            except Exception as e:
                logger.error(f"Error checking node {node_url}: {str(e)}")
                needs_update = self._handle_node_error(node_key, node_info)
                
            if needs_update:
                self._save_node_info(node_key, node_info)
        
        def _update_node_status(self, node_info: dict, response_data: dict) -> bool:
            """更新节点状态和角色"""
            needs_update = False
            
            # 更新状态为正常
            if node_info.get("status") != 1:
                node_info["status"] = 1
                needs_update = True
                
            # 更新角色
            if "node" in response_data and "state" in response_data["node"]:
                new_role = ServerRole.MASTER if response_data["node"]["state"] == "leader" else ServerRole.SLAVE
                if node_info.get("role") != new_role:
                    node_info["role"] = int(new_role)
                    needs_update = True
                    
            return needs_update
        
        def _handle_node_error(self, node_key: str, node_info: dict) -> bool:
            """处理节点错误"""
            self.node_error_counts[node_key] += 1
            if self.node_error_counts[node_key] >= 5 and node_info.get("status") != 0:
                node_info["status"] = 0
                return True
            return False
        
        def _save_node_info(self, node_key: str, node_info: dict) -> None:
            """保存节点信息到etcd"""
            try:
                self.etcd_client.put(node_key, str(node_info))
                logger.info(f"Updated node {node_key} with new status and role")
            except Exception as e:
                logger.error(f"Failed to update node {node_key} in etcd: {str(e)}")

        try:
            # 获取所有节点信息
            for value, metadata in self.etcd_client.get_prefix("/instances/"):
                if not value:
                    continue
                
                # TODO：需要支持vdb get node
                # try:
                #     node_info = eval(value.decode('utf-8'))
                #     node_key = metadata.key.decode('utf-8')
                #     process_node(node_key, node_info)
                # except Exception as e:
                #     logger.error(f"Error processing node: {str(e)}")
                #     continue
                    
        except Exception as e:
            logger.error(f"Error updating node states: {str(e)}")

    def setup_routes(self):
        @self.app.get("/getNodeInfo", response_model=ResponseModel)
        async def get_node_info(instanceId: str, nodeId: str):
            try:
                etcd_key = f"/instances/{instanceId}/nodes/{nodeId}"
                value, _ = self.etcd_client.get(etcd_key)
                
                if not value:
                    return ResponseModel(
                        retCode=1,
                        msg=f"Node not found: {nodeId}"
                    )

                node_info = eval(value.decode('utf-8'))
                return ResponseModel(
                    retCode=0,
                    msg="Node info retrieved successfully",
                    data={
                        "instanceId": instanceId,
                        "nodeId": nodeId,
                        "nodeInfo": node_info
                    }
                )
            except Exception as e:
                logger.error(f"Error accessing etcd: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/addNode", response_model=ResponseModel)
        async def add_node(request: NodeRequest):
            try:
                etcd_key = f"/instances/{request.instanceId}/nodes/{request.nodeId}"
                node_info = {
                    "instanceId": request.instanceId,
                    "nodeId": request.nodeId,
                    "url": request.url,
                    "role": request.role.value,
                    "status": request.status,
                }
                self.etcd_client.put(etcd_key, str(node_info))
                return ResponseModel(
                    retCode=0,
                    msg="Node added successfully"
                )
            except Exception as e:
                logger.error(f"Error accessing etcd: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.delete("/removeNode", response_model=ResponseModel)
        async def remove_node(instanceId: str, nodeId: str):
            try:
                etcd_key = f"/instances/{instanceId}/nodes/{nodeId}"
                self.etcd_client.delete(etcd_key)
                return ResponseModel(
                    retCode=0,
                    msg="Node removed successfully"
                )
            except Exception as e:
                logger.error(f"Error accessing etcd: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/getInstance", response_model=ResponseModel)
        async def get_instance(instanceId: str):
            try:
                logger.info(f"Getting instance information for instanceId: {instanceId}")
                etcd_prefix = f"/instances/{instanceId}/nodes/"
                
                nodes = []
                for value, metadata in self.etcd_client.get_prefix(etcd_prefix):
                    if value:
                        try:
                            node_info = eval(value.decode('utf-8'))
                            nodes.append(node_info)
                        except Exception as e:
                            logger.warning(f"Invalid node info format: {str(e)}")
                            continue

                return ResponseModel(
                    retCode=0,
                    msg="Instance info retrieved successfully",
                    data={
                        "instanceId": instanceId,
                        "nodes": nodes
                    }
                )
            except Exception as e:
                logger.error(f"Error accessing etcd: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

    def cleanup(self):
        """清理资源"""
        self.running = False
        if hasattr(self, 'update_thread'):
            self.update_thread.join(timeout=1)

    def run(self, host: str = "0.0.0.0", port: int = 80):
        try:
            uvicorn.run(self.app, host=host, port=port)
        finally:
            self.cleanup()


if __name__ == "__main__":
    server = MasterServer("localhost:2379")
    server.run(port=8100)