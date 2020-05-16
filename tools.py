import os
import random
import asyncio
import aiohttp

import requests
from lxml import etree
from typing import Union

from setting import user_agents, logger, invalid_chars_in_path


def dl_header() -> dict:
    """ 随机返回图片下载请求头 """
    header = {
        "user-agent": random.choice(user_agents),
        "Referer": "https://www.mzitu.com"
    }
    return header


def craw_header() -> dict:
    """ 随机返回主站网页请求头 """
    header = {
        "user-agent": random.choice(user_agents),
        "Host": "www.mzitu.com",
    }
    return header


def get_session():
    """todo: 自定义主站网页反反爬方法"""
    timeout = aiohttp.ClientTimeout(total=10, connect=3)
    connector = aiohttp.TCPConnector(limit=5)
    session = aiohttp.ClientSession(connector=connector, headers=craw_header(), timeout=timeout)
    return session


def get_all_collection_num():
    """获取所有的合集编号"""
    init_urls = ["https://www.mzitu.com/all/", ]
    collection_nums = list()

    for init_url in init_urls:
        res = requests.get(init_url, headers=craw_header())
        if res.status_code != 200:
            logger.warning(f"请求合集目录页失败，状态码：{res.status_code}")
            return None
        html = etree.HTML(res.text)
        collection_urls = html.xpath("//div[@class='all']//a/@href")

        collection_nums.extend([url.split("/")[-1] for url in collection_urls])
    return collection_nums


async def get_collection_base_data(collection_num, retry=3) -> Union[dict, None]:
    """ 异步获取合集的基础信息 """
    await asyncio.sleep(random.uniform(.5, 1.5))
    collection_url = f"https://www.mzitu.com/{collection_num}"
    logger.debug(f"合集地址：{collection_url}")

    http_session = get_session()
    for i in range(1, retry + 1):
        try:
            resp = await http_session.get(collection_url)
            if resp.status != 200:
                logger.warning(f"error：获取合集{collection_num}失败，状态码：{resp.status}")

                if resp.status == 429:
                    logger.info(f"{collection_url}触发网站反爬机制，睡眠10秒，重试-{i}")
                    await asyncio.sleep(10)
                    continue

                return None

            text = await resp.text()

            # todo:抽象成通用解析单元
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

            resp.close()
            await http_session.close()
            return res

        except asyncio.TimeoutError:
            logger.error("timeout-1")
            continue
        except asyncio.CancelledError:
            logger.error(f"timeout-2")
            continue


async def collection_downloader(base_path, collection_name, url_list: list, retry=3):
    """ 异步图片下载器"""
    async with aiohttp.ClientSession(headers=dl_header()) as session:
        logger.info(f"开始下载合集图片：{collection_name}")

        # 此合集下载失败的图片数量，若大于 10，返回None 合集下载失败
        fail_count = 0
        for img_url in url_list:
            await asyncio.sleep(random.uniform(.5, 2.5))

            file_name = img_url.split("/")[-1]

            # 检查文件夹命名的格式
            for char in invalid_chars_in_path:
                if char in collection_name:
                    collection_name = collection_name.replace(char, "")
            dir_path = os.path.join(base_path, collection_name)

            if not os.path.exists(dir_path):
                try:
                    os.mkdir(dir_path)
                except NotADirectoryError:
                    os.mkdir(os.path.join(base_path, "unknown"))

            file_path = os.path.join(dir_path, file_name)

            # 如果已经下载就跳过
            if os.path.exists(file_path):
                continue

            # 每张图片有 3 次下载机会，若全部失败，fail_count 加一
            for i in range(1, retry+1):
                try:
                    resp = await session.get(img_url)
                    if resp.status != 200:
                        logger.warning(f"下载限制，状态码：{resp.status}")

                        if resp.status == 429:
                            logger.info(f"触发网站反爬机制，睡眠20秒，重试-{i}")
                            await asyncio.sleep(20)
                            continue

                    with open(file_path, 'wb') as fd:
                        while True:
                            chunk = await resp.content.read(1024)
                            if not chunk:
                                break
                            fd.write(chunk)
                    logger.debug(f"{file_name}已保存")
                    resp.close()
                    break

                except asyncio.TimeoutError:
                    logger.warning(f"{img_url} -timeout")
                    continue
            else:
                fail_count += 1

            if fail_count >= 10:
                return False

        return True
