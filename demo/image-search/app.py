import traceback
import os
import logging as logger
from typing import Optional

from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from image_eb import extract_features
from client import VectorDBClient, IndexType


app = FastAPI()

# 静态文件和模板配置
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# 初始化向量数据库客户端
db_sdk = VectorDBClient()

@app.get("/")
async def index(request: Request):
    """首页路由"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/search")
async def search_image(image: UploadFile = File(...)):
    """
    图像搜索接口
    """
    try:
        # 保存上传的图片
        image_path = os.path.join('static/images', 'temp_image.jpg')
        with open(image_path, "wb") as buffer:
            content = await image.read()
            buffer.write(content)

        image_features = extract_features(image_path)
        
        search_result = db_sdk.search(
            vectors=image_features.tolist(), 
            k=5, 
            index_type=IndexType.FLAT
        )

        image_urls = [
            f"/static/images/{int(image_id)}" 
            for image_id in search_result.vectors
        ]

        return {
            "data": search_result,
            "distances": search_result.distances,
            "image_urls": image_urls
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload")
async def upload_image(
    image: UploadFile = File(...),
    image_id: int = Form(...)
):
    """
    图像上传接口
    """
    try:
        image_path = os.path.join('static/images', str(image_id))
        with open(image_path, "wb") as buffer:
            content = await image.read()
            buffer.write(content)

        image_features = extract_features(image_path)
        logger.error(len(image_features.tolist()))
        upsert_result = db_sdk.upsert(
            id=image_id,
            vectors=image_features.tolist(),
            index_type=IndexType.FLAT
        )

        if upsert_result.retcode != 0:
            raise HTTPException(
                status_code=500, 
                detail=upsert_result.error_msg
            )

        return {"message": "Image uploaded successfully", "id": image_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)