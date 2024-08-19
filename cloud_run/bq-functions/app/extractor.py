from newspaper import Article
from newspaper import Config

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
config = Config()
config.browser_user_agent = user_agent
import nltk


import time
def get_article(url):
    # nltk.download('punkt_tab')
    start_time = time.time()
    download_time = parse_time = nlp_time = None
    
    try:
        article = Article(url, config=config)
        
        start_download = time.time()
        article.download()
        download_time = time.time() - start_download
        
        start_parse = time.time()
        article.parse()
        parse_time = time.time() - start_parse
        
        start_nlp = time.time()
        article.nlp()
        nlp_time = time.time() - start_nlp
        
        data = {
            "title": article.title,
            "text": article.text,
            "summary": article.summary,
        }
    except Exception as e:
        data = {
            "title": "ERROR",
            "text": str(e),
            "summary": "ERROR",
        }
    finally:
        total_time = time.time() - start_time
        data = {
            **data,
            "download": download_time,
            "parse": parse_time,
            "nlp": nlp_time,
            "total": total_time
        }
        return data
    
from concurrent.futures import ThreadPoolExecutor, as_completed

def scrape_articles_parallel(urls, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(get_article, url): url for url in urls}
        for future in as_completed(future_to_url):
            results.append(future.result())
    return results