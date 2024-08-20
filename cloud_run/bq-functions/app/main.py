from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Union, Optional
from app.extractor import scrape_articles_parallel
import nltk

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

class ErrorResponse(BaseModel):
    errorMessage: str

@app.post("/", response_model=Union[BatchAddResponse, ErrorResponse])
async def batch_add(request: BatchAddRequest):
    try:
        nltk.download('punkt_tab')
        list_urls = [call[0] for call in request.calls if isinstance(call[0], str)]
        threads = request.calls[0][1] if len(request.calls) > 0 and len(request.calls[0]) > 1 else None
        print("batch size", len(request.calls))
        if not list_urls:
            raise ValueError("No valid URLs provided")
        
        if not isinstance(threads, int) and threads is not None:
            raise ValueError("Invalid thread count")
        
        articles = scrape_articles_parallel(list_urls, threads)
        return BatchAddResponse(replies=[ArticleData(**article) for article in articles])
    except Exception as e:
        return ErrorResponse(errorMessage=str(e))