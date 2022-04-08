import csv
import json
import sqlite3
import zlib
from time import sleep
from datetime import date, timedelta, datetime
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from lxml import html
'''
url_daynews = 'https://classic.newsru.com/allnews/14jun2000/'
start_day = 14jun2000
end_day = 31may2021
'''
DELAY = 0

s = requests.Session()
s.headers.update({
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
    })

@dataclass
class Article:
    time_stamp: str = ''
    url: str = ''
    title: str = ''
    content: str = ''    
    image_url: str = ''
    rubric: str = ''
    def toJSON(self):
        return json.dumps(self, default=lambda dc: asdict(dc))

def save_csv(file, results):
    with open(file, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, json.loads(zlib.decompress(results[0]).decode()).keys())
        writer.writeheader()
        for r in results:
            writer.writerow(json.loads(zlib.decompress(r).decode()))

def save_sqlite(results):
    conn = sqlite3.connect('newsru_com.db', check_same_thread = False)
    with conn:
        conn.executescript('PRAGMA journal_mode = wal; PRAGMA synchronous = 1;')
        conn.execute('''CREATE TABLE IF NOT EXISTS newsru_com(time_stamp TEXT, url TEXT UNIQUE ON CONFLICT IGNORE, title TEXT,
                                                              content TEXT, image_url TEXT, rubric TEXT)''')
        conn.executemany('''insert into newsru_com values (?, ?, ?, ?, ?, ?)''', ([str(x) for x in json.loads(zlib.decompress(r).decode()).values()] for r in results))
        conn.execute('PRAGMA optimize;')
    print(f'SQLite3 commit {len(results)} pages.')

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_url(url):
    global DELAY
    while True:
        resp = s.get(url)
        #print(f"{resp.elapsed > timedelta(seconds=0.4)} {resp.status_code} {resp.elapsed} {resp.url}")
        DELAY = 0.4 if resp.elapsed > timedelta(seconds=0.4) else 0
        sleep(DELAY)
        if resp.status_code == 200:
            return resp
        if resp.status_code == 404 and 'Извините, запрашиваемая страница не найдена' in resp.text:
            return 404
        if 'Ошибка сервера.' in resp.text:
            return 404
        print(f'{resp.status_code=} {resp.url=} {resp.elapsed} Sleep 5 sec.')
        sleep(5)

def parse_article(url_end):
    url = f'https://classic.newsru.com{url_end}'
    resp = get_url(url)
    if resp == 404:
        article = Article()
        article.url = f"https://www.newsru.com{url_end}"
        return article
    tree = html.fromstring(resp.text)
    article = Article()
    time_stamp = tree.xpath("//td[@class='article-date']/text()")[0].strip().lstrip('время публикации:')
    months = {'января': '01',
             'февраля': '02',
             'марта': '03',
             'апреля': '04',
             'мая': '05',
             'июня': '06',
             'июля': '07',
             'августа': '08',
             'сентября': '09',
             'октября': '10',
             'ноября': '11',
             'декабря': '12'}
    try:
        for month in months:
            if month in time_stamp:
                time_stamp = time_stamp.replace(month, months[month])
        time_stamp = datetime.strptime(time_stamp, "%d %m %Y г., %H:%M")
    except:
        time_stamp = datetime.strptime(time_stamp, "%d %B %Y г., %H:%M")
    article.time_stamp = time_stamp.astimezone().isoformat()
    article.url = f"https://www.newsru.com{url_end}"
    article.title = tree.xpath("//div[@class='article']//h1//text()")[0].strip()
    text = tree.xpath("//div[@class='article-text']//p")
    article.content = '/n/n'.join(e.text_content().strip() for e in text)
    image_url = tree.xpath("//div[@class='article-text']//div[@class='article-list-img']//img/@src")
    article.image_url = ', '.join(image_url) if image_url else '-'
    rubric = tree.xpath("//nobr/a//text()")
    article.rubric = rubric[0].strip() if rubric else ''
    return article

def parse_day(day):
    day_url = f'https://classic.newsru.com/allnews/{day}/'   
    resp = get_url(day_url)
    tree = html.fromstring(resp.text)
    url_ends = tree.xpath("//td[@class='index-news-content']/a[@class='index-news-title']/@href")
    url_ends = [url for url in url_ends if not('https://www.inopressa.ru' in url or '//www.meddaily.ru' in url or '/.html' in url)] # remove bad urls
    return url_ends

def work_day(day):
    url_ends = parse_day(day)
    results, futures = [], []
    with ThreadPoolExecutor(4) as executor:
        for url_end in url_ends:
            future = executor.submit(parse_article, url_end)
            futures.append(future)
            sleep(DELAY)
        for r in as_completed(futures):
            results.append(r.result())
    results = sorted(results, key=lambda x: x.time_stamp) # sort
    results = [zlib.compress(x.toJSON().encode()) for x in results] # compress
    return results

if __name__ == '__main__':
    start_date = date(2000, 6, 14)
    end_date = date(2021, 6, 1)
    all_days = end_date - start_date
    for i, single_date in enumerate(daterange(start_date, end_date), start=1):
        day = single_date.strftime("%d%b%Y")
        print(f"{i}/{all_days} {day}")        
        day_results = work_day(day)
        sleep(DELAY)
        #save_csv('newsru_com.csv', day_results)
        save_sqlite(day_results)
