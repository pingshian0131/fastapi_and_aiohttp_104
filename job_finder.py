import asyncio
import random
import time
import aiohttp
from pydantic import BaseModel, Field
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import requests
from bs4 import BeautifulSoup
import pandas as pd

app = FastAPI()

def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    print(soup)
    title = soup.find("job-header__title")
    content = soup.find("p", class_="mb-5 r3 job-description__content text-break")
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

url = "https://www.104.com.tw/jobs/search/?jobsource=index_s&keyword=django&mode=s&page=1"
prefix = "https:"

@app.get("/async/api/104/django/1/")
async def get_django_job():
    start_time = time.time()
    print(f"---------- start_time: {start_time}s ----------")
    # get page1 all_url
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    links_ele = soup.find_all("a", {"class": "jb-link"}, href=True)
    links = [ele["href"] for i, ele in enumerate(links_ele)]
    titles = [ele.text for i, ele in enumerate(links_ele)]
    print(titles)
    print(links)

    # start parse_and_fetch
    # jobs = await aiohttp_104(links)

    # make FastAPI return objects
    # res = [Job(title=job.get("title"), content=job.get("content")) for job in jobs]
    res = [Job(title=title, content=link) for title, link in zip(titles, links)]
    json_compatible_item_data = jsonable_encoder(res)
    print(f"---------- execute_time: {time.time() - start_time}s ----------")
    return JSONResponse(content=json_compatible_item_data)


@app.get("/sync/api/104/django/{page}/")
def get_jobs(page: int):
    url = f"https://www.104.com.tw/jobs/search/?ro=0&kwop=7&keyword=django&expansionType=area%2Cspec%2Ccom%2Cjob%2Cwf%2Cwktm&order=15&asc=0&lang=1&page={page}&mode=s&jobsource=2018indexpoc&langFlag=0&langStatus=0&recommendJob=1&hotJob=1"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    links_ele = soup.find_all("a", {"class": "js-job-link"}, href=True)
    links = [prefix + ele["href"] for i, ele in enumerate(links_ele)]

    jobs = []
    ids = []
    titles = []
    contents = []
    for i, link in enumerate(links):
        s = 1 + random.random()
        time.sleep(s)
        r_link = requests.get(link)
        if r_link.status_code == 200:
            try:
                soup = BeautifulSoup(r_link.text, "html.parser")
                title = soup.find("h1").text
                content = soup.find("p", class_="mb-5 r3 job-description__content text-break").text
                ids.append(i+1)
                titles.append(title)
                contents.append(content)
                jobs.append({"title": title, "content": content})
            except Exception as e:
                print(str(e))

    df = pd.DataFrame(data={'id': ids, 'title': titles, 'contents': contents})
    f_path = f'./out_p{page}.xlsx'
    df.to_excel(f_path, sheet_name='jobs', engine='openpyxl', index=False)

    return jobs

