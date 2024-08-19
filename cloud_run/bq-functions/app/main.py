from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Union
from app.extractor import scrape_articles_parallel
import nltk
from typing import Optional


app = FastAPI()

class BatchAddRequest(BaseModel):
    calls: List[List[Union[int, str, None]]]

class ArticleData(BaseModel):
    title: str
    text: str
    summary: str
    download: Optional[float]
    parse: Optional[float]
    nlp: Optional[float]
    total: float

class BatchAddResponse(BaseModel):
    replies: List[ArticleData]

@app.post("/")
async def batch_add(request: BatchAddRequest):
    try:
        nltk.download('punkt_tab')
        print(request.calls)
        list_urls = [call[0] for call in request.calls]
        print("okkkkkkkkk")
        threads = request.calls[0][1]
        print(len(list_urls))
        articles = scrape_articles_parallel(list_urls, threads)
        return BatchAddResponse(replies=articles)
    except Exception as e:
        return {
            "errorMessage": str(e)
        }