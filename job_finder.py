import asyncio
import time
import aiohttp
from pydantic import BaseModel, Field
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import requests
from bs4 import BeautifulSoup

app = FastAPI()

def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("title").text
    content = soup.find("meta", {"property": "og:description"}).get("content")
    return {"title": title, "content": content}

async def aiohttp_fetch(client, link):
    print("exec_aiohttp_fetch")
    async with client.get(link) as r:
        assert r.status == 200
        return await r.text()

async def aiohttp_104(links):
    print("exec_aiohttp_104")
    res = []
    async with aiohttp.ClientSession() as client:
        for link in links:
            print(link)
            html = await aiohttp_fetch(client, link)
            res.append(parse_html(html))
    return res

class Job(BaseModel):
    title: str = Field(title="職稱")
    content: str = Field(title="工作內容")

url = "https://www.104.com.tw/jobs/search/?ro=0&keyword=django&expansionType=area%2Cspec%2Ccom%2Cjob%2Cwf%2Cwktm&order=14&asc=0&page=1&mode=s&jobsource=2018indexpoc&langFlag=0&langStatus=0&recommendJob=1&hotJob=1"
prefix = "https:"

@app.get("/api/104/django/1/")
async def get_django_job():
    start_time = time.time()
    print(f"---------- start_time: {start_time}s ----------")
    # get page1 all_url
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    links_ele = soup.find_all("a", {"class": "js-job-link"}, href=True)
    links = [prefix + ele["href"] for i, ele in enumerate(links_ele)]

    # start parse_and_fetch
    jobs = await aiohttp_104(links)

    # make FastAPI return objects
    res = [Job(title=job.get("title"), content=job.get("content")) for job in jobs]
    json_compatible_item_data = jsonable_encoder(res)
    print(f"---------- execute_time: {time.time() - start_time}s ----------")
    return JSONResponse(content=json_compatible_item_data)

