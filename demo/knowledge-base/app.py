import traceback
import logging as logger
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
import google.generativeai as genai
from markdown_processor import markdown_to_html, split_html_into_segments, vectorize_segments
from client import VectorDBClient, IndexType
import json

app = FastAPI()

# 静态文件和模板配置
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# 初始化客户端
db_sdk = VectorDBClient(base_url="http://127.0.0.1:8000")

# 配置 Gemini
genai.configure(api_key='')
model = genai.GenerativeModel('gemini-pro')

class SearchQuery(BaseModel):
    search: str

@app.get("/")
async def index(request: Request):
    """首页路由"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    """上传并处理文档"""
    try:
        content = await file.read()
        markdown_text = content.decode('utf-8')
        html_text = markdown_to_html(markdown_text)
        segments = split_html_into_segments(html_text)
        vectors = vectorize_segments(segments)

        # 上传向量到数据库
        for i, (segment, vector) in enumerate(zip(segments, vectors)):
            vector_id = i + 1
            db_sdk.upsert(
                vectors=vector.tolist(), 
                id=vector_id, 
                index_type=IndexType.FLAT,
                extra_fields={"text": segment}
            )

        return JSONResponse(content={'message': '文件已处理并上传向量到数据库'})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search")
async def search(query: SearchQuery):
    """搜索接口"""
    try:
        # 添加指令并向量化查询
        instruction = "为这个句子生成表示以用于检索相关文章："
        search_text_with_instruction = instruction + query.search
        search_vector = vectorize_segments([search_text_with_instruction])[0].tolist()

        search_results = db_sdk.search(
            vectors=search_vector, 
            k=5, 
            index_type=IndexType.FLAT
        )

        context = "请基于以下内容回答问题：\n\n"
        for result_id in search_results.vectors:
            if result_id != -1:
                query_result = db_sdk.query(id=result_id)
                if query_result and 'text' in query_result:
                    context += query_result['text'] + "\n\n"

        # 使用 Gemini 生成回答
        prompt = f"{context}\n问题：{query.search}\n回答："
        response = model.generate_content(prompt)
        
        return {"answer": response.text}
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)