import os
import random
import logging
import asyncio
import aiohttp
import requests
from lxml import etree
from typing import Union

logger = logging.getLogger("logger")


def get_all_collection_num():
    """获取所有的合集编号"""

    init_urls = ["https://www.mzitu.com/all/", ]
    collection_nums = list()

    for init_url in init_urls:
        res = requests.get(init_url, headers=host_headers())
        if res.status_code != 200:
            logger.warning(f"请求合集目录页失败，状态码：{res.status_code}")
            return None
        html = etree.HTML(res.text)
        collection_urls = html.xpath("//div[@class='all']//a/@href")

        collection_nums.extend([url.split("/")[-1] for url in collection_urls])
    logger.debug(collection_nums)
    return collection_nums


def host_headers() -> dict:
    """ 随机返回请求头 """
    headers = [
        {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
            "Referer": "https://www.mzitu.com"
        },
        {
            "user-agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
            "Referer": "https://www.mzitu.com"
        },
        {
            "user-agent": "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
            "Referer": "https://www.mzitu.com"
        },
        {
            "user-agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
            "Referer": "https://www.mzitu.com"
        },
    ]
    return random.choice(headers)


def get_headers_1() -> dict:
    """ 随机返回请求头 """
    headers = [
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
            "Host": "www.mzitu.com",
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
            "Host": "www.mzitu.com",

        },
        {
            "User-Agent": "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
            "Host": "www.mzitu.com",

        },
        {
            "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
            "Host": "www.mzitu.com",

        },
    ]
    return random.choice(headers)


async def get_collection_base_data(http_session, collection_num) -> Union[dict, None]:
    """ 异步获取合集的基础信息 """
    await asyncio.sleep(random.uniform(.5, 1.5))
    collection_url = f"https://www.mzitu.com/{collection_num}"
    logger.debug(f"合集地址：{collection_url}")

    async with http_session.get(collection_url) as resp:
        logger.debug(resp.status)
        if resp.status != 200:
            logger.warning(f"error：获取合集{collection_num}失败，状态码：{resp.status}")
            if resp.status == 429:
                logger.info(f"触发网站反爬机制，睡眠10秒")
                await asyncio.sleep(10)
            return None

        text = await resp.text()
        html = etree.HTML(text)
        tag_names = html.xpath("//div[@class='main-tags']/a/text()")
        total_num = html.xpath("//div[@class='pagenavi']/a[last()-1]/span/text()")[0]
        name = html.xpath("//h2[@class='main-title']/text()")[0]

        img_first_url = html.xpath("//div[@class='main-image']/p/a/img/@src")[0]

        splits_1 = os.path.split(img_first_url)
        url_prefix = splits_1[0] + "/" + splits_1[1][:3]
        url_suffix = splits_1[1][5:]

        splits_2 = img_first_url.split("/")
        year = splits_2[3]
        month = splits_2[4]
        day = splits_2[5][:2]

        res = {
            "collection_num": collection_num,
            "name": name,
            "total_num": total_num,
            "year": year,
            "month": month,
            "day": day,
            "url_prefix": url_prefix,
            "url_suffix": url_suffix,
            "tag_names": tag_names,
        }
        logger.debug(f"合集信息：{res}")
        return res


async def download_worker(base_path, url_queue: asyncio.Queue):
    """ 异步图片下载器"""
    while True:
        await asyncio.sleep(random.uniform(.5, 2.5))
        collection_name, img_url = await url_queue.get()
        logger.debug(f"爬取图片：{img_url}")
        file_name = img_url.split("/")[-1]
        dir_path = os.path.join(base_path, collection_name)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        file_path = os.path.join(dir_path, file_name)
        logger.debug(f"存放路径：{file_path}")

        async with aiohttp.ClientSession(headers=host_headers()) as session:
            async with session.get(img_url) as resp:
                logger.debug(img_url)
                logger.debug(resp.status)
                if resp.status != 200:
                    logger.warning(f"error：下载限制，请更换IP地址,状态码：{resp.status}")
                    url_queue.task_done()
                    continue
                logger.debug(file_path)
                with open(file_path, 'wb') as fd:
                    while True:
                        chunk = await resp.content.read(1024)
                        if not chunk:
                            break
                        fd.write(chunk)
                logger.debug(f"{file_name}已保存")
                url_queue.task_done()


# if __name__ == '__main__':
    # asyncio.run(get_collection_base_data(230639))
    # q = asyncio.Queue()
    # q.put(("133938", "https://i3.mmzztt.com/2018/05/10b01.jpg"))
    # q.put(("133938", "https://i3.mmzztt.com/2018/05/10b02.jpg"))
    # q.put(("133938", "https://i3.mmzztt.com/2018/05/10b03.jpg"))
    # print(asyncio.run(download_worker("D:/mzitu", q)))
