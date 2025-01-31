import logging
from enum import IntEnum
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
        self.setup_routes()

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

    def run(self, host: str = "0.0.0.0", port: int = 80):
        uvicorn.run(self.app, host=host, port=port)


if __name__ == "__main__":
    server = MasterServer("localhost:2379")
    server.run(port=8100)