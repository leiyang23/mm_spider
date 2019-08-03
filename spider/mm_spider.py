import os
import re
import random
import asyncio
import aiohttp
import requests

from lxml import etree
from typing import Union
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Collection, Tag, Image, base, DownloadRecord


class MeiSpider:
    """妹子图全站下载
    使用说明：1.初始化数据库 2.将所有图片信息爬取到数据库 3.根据数据库信息下载图片到本地

    """

    def __init__(self, db_path, req_cor_num: int = 1, dl_cor_num: int = 5):

        # 下载图片时开启的协程数
        if dl_cor_num >= 30:
            print("爬亦有道，下载最大只能为 30.")
            self.dl_cor_num = 30
        else:
            self.dl_cor_num = dl_cor_num

        # 进行网页获取时开启的协程数
        if req_cor_num >= 50:
            print("爬亦有道，最大只能为 50.")
            self.req_cor_num = 50
        else:
            self.req_cor_num = req_cor_num

        # 文件存放路径
        self.file_path = "/mm_images"
        self.db_path = db_path
        self.session = None
        self._init_conn_db()

    def _init_conn_db(self):
        """初始化连接数据库"""

        try:
            engine = create_engine(f"sqlite:{self.db_path}", echo=False)
            db_session = sessionmaker(bind=engine)
            self.session = db_session()
            # 创建表（如果表已经存在，则不会创建）
            base.metadata.create_all(engine)
            print("初始化连接数据库完毕")
        except ImportError as e:
            if e.name == '_sqlite3':
                print(f"执行 yum install sqlite-devel，之后重新编译安装Python")
            else:
                print(f"请检查是否安装此模块：{e.name}")
            exit()

    def _insert_collection_data_to_db(self, collection_data: tuple, collection_num=''):
        """ 添加合集的数据"""
        # 添加合集的标签
        name, tag_names, total_num, img_first_url, width, height = collection_data
        tags = []
        for tag_name in tag_names:
            t = self.session.query(Tag).filter_by(tag_name=tag_name)
            if t.count() == 0:  # tag不存在
                self.session.add(Tag(tag_name=tag_name))
                self.session.commit()
                tags.append(self.session.query(Tag).filter_by(tag_name=tag_name).first())
            else:  # tag 存在
                tags.append(t.first())

        collection = Collection(collection_num=collection_num, name=name, total_num=total_num, tags=tags, )
        self.session.add(collection)
        self.session.commit()

        #  添加该合集的图片信息
        images = []
        for count in range(1, int(total_num) + 1):
            if 1 <= count <= 9:
                index = '0' + str(count)
            else:
                index = str(count)
            info = img_first_url.split("/")
            year = info[-3]
            month = info[-2]
            day = info[-1][:2]

            img_url = f"https://i.meizitu.net/{year}/{month}/{day}{info[-1][2]}{index}.{info[-1].split('.')[-1]}"
            image = Image(year=year, month=month, day=day, width=width, height=height, meizitu_url=img_url,
                          collection_num=collection_num)
            images.append(image)

        self.session.add_all(images)
        # self.session.commit()

        download_record = self.session.query(DownloadRecord).filter_by(collection_num=collection_num)
        download_record.update({'status': 1})
        self.session.commit()

    def update_collection_record(self):
        """
        若合集记录数目少于 2000，将获取到的妹子图网站的全部合集覆盖写入
        否则，进行增量更新
        """
        collection_nums = self.get_all_collection_num()
        if collection_nums is None:
            print("未能获取当前最新数据")
            return None

        dl_records = self.session.query(DownloadRecord).filter_by()
        count = 0
        if dl_records.count() > 2000:  # 下载记录表记录大于2000 说明此时要更新
            print("更新中···")
            for collection_num in collection_nums:
                # 若已存在，跳过
                if self.session.query(DownloadRecord).filter_by(collection_num=collection_num).count() > 0:
                    continue

                # 合集编号 格式不对
                if len(collection_num) < 2:
                    continue

                self.session.add(DownloadRecord(collection_num=collection_num))
                count += 1
                if count % 100 == 0:
                    self.session.flush()
            self.session.commit()
        else:  # 少于 2000，进行初始化
            print("初始化数据···")
            self.session.query(DownloadRecord).filter_by().delete()
            self.session.query(Tag).filter_by().delete()
            self.session.query(Image).filter_by().delete()
            self.session.query(Collection).filter_by().delete()

            self.session.add_all([DownloadRecord(collection_num=c) for c in collection_nums if len(c) > 2])
            self.session.commit()

    async def worker_for_collection_data(self, queue):
        """  进行合集的基本信息收录"""
        while True:
            collection_num = await queue.get()
            print(f"{collection_num}开始。。")
            res = await self.get_collection_base_data(collection_num)
            if res is None:  # 获取合集基本信息失败
                queue.task_done()
                continue
            self._insert_collection_data_to_db(res, collection_num=collection_num)
            print(f"{collection_num}结束。。")
            queue.task_done()

    async def _main_save2db(self):
        """获取整个妹子图的合集到数据库"""

        #  1、更新合集记录表
        self.update_collection_record()

        # 2、对未收录的合集进行信息收集
        queue = asyncio.Queue()
        dl_records = self.session.query(DownloadRecord).filter_by(status=0)

        for dl_record in dl_records:
            queue.put_nowait(dl_record.collection_num)
        print("炮弹装填完毕")

        tasks = []
        for _ in range(self.req_cor_num):
            task = asyncio.create_task(self.worker_for_collection_data(queue))
            tasks.append(task)
        await queue.join()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("合集信息已全部写入数据库")

    async def _re_get_error_collection_to_db(self):
        """ 对于已收录但未进行基本数据收集的合集进行补录"""
        error_collections = self.session.query(DownloadRecord).filter_by(status=0)
        for collection in error_collections:
            res = await self.get_collection_base_data(collection.collection_num)
            if res is None:
                print(f"合集{collection.collection_num}存在问题，跳过")
                continue
            self._insert_collection_data_to_db(res, collection_num=collection.collection_num)

    async def _main_save2local(self):
        """将没有下载过的图片下载到本地"""
        queue = asyncio.Queue()
        dl_records = self.session.query(DownloadRecord).filter_by(dl_status=0)

        for dl_record in dl_records:
            if dl_record.status == 0:
                print(f"此合集{dl_record.collection_num}基本信息还未收录")
                continue
            collection = self.session.query(Collection).filter_by(collection_num=dl_record.collection_num).first()

            total_num = int(collection.total_num)
            img_path = self.file_path + "/" + re.sub(r"[/\\？?*:.<>|]", '', collection.name)
            try:
                os.mkdir(img_path)
            except FileExistsError:
                # 如果该文件夹下文件数量大致等于合集图片数，则跳过,否则重新下载
                if total_num - len(os.listdir(img_path)) <= 5:
                    dl_record.dl_status = 1
                    self.session.commit()
                    continue

            except NotADirectoryError:
                continue

            images = self.session.query(Image).filter_by(collection_num=dl_record.collection_num)
            for j in images:
                queue.put_nowait((img_path, j.meizitu_url))
        print("炮弹装填完毕")

        tasks = []
        for _ in range(self.dl_cor_num):
            task = asyncio.create_task(self.worker_for_file_download(queue))
            tasks.append(task)

        await queue.join()

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        print("图片下载完毕")

    def save2db(self):
        """ 获取当前的合集数，进行增量添加"""
        asyncio.run(self._main_save2db())

    def re_get_error_collection_to_db(self):
        """将信息获取失败的合集进行重新收录"""
        asyncio.run(self._re_get_error_collection_to_db())

    def save2local(self, file_path):
        """根据数据库中登记的合集进行下载,跳过已经下载合集"""

        if not os.path.exists(file_path):
            os.mkdir(file_path)
            print("指定路径不存在，已经新建文件夹")
        self.file_path = file_path  # 图片存放的路径：绝对路径

        collections = self.session.query(Collection).filter_by()
        if collections.count() < 1000:
            # 如果收录的合集数目太少，则重新进行收录
            asyncio.run(self._main_save2db())

        asyncio.run(self._main_save2local())

    @staticmethod
    async def worker_for_file_download(queue: asyncio.Queue):
        """ 异步图片下载器"""
        while True:
            info = await queue.get()
            img_path, img_url = info
            name = img_url.split("/")[-1]
            async with aiohttp.ClientSession(headers=MeiSpider.get_headers()) as session:
                async with session.get(img_url) as resp:
                    print(img_url)
                    if resp.status != 200:
                        print(f"error：下载限制，请更换IP地址,状态码：{resp.status}")
                        queue.task_done()
                        continue
                    print(img_path + "/" + name)
                    with open(img_path + "/" + name, 'wb') as fd:
                        while True:
                            chunk = await resp.content.read(1024)
                            if not chunk:
                                break
                            fd.write(chunk)
                    queue.task_done()

    @staticmethod
    def get_all_collection_num():
        """获取所有的合集编号"""
        init_url = "https://www.mzitu.com/all/"

        res = requests.get(init_url, headers=MeiSpider.get_headers())
        if res.status_code != 200:
            print(f"请求合集目录页失败，状态码：{res.status_code}")
            return None
        html = etree.HTML(res.text)
        collection_urls = html.xpath("//div[@class='all']//a/@href")

        collection_nums = [collection_url.split("/")[-1] for collection_url in collection_urls]
        return collection_nums

    @staticmethod
    async def get_collection_base_data(collection_num) -> Union[tuple, None]:
        """ 异步获取合集的基础信息 """
        await asyncio.sleep(random.uniform(.2, .4))
        collection_url = f"https://www.mzitu.com/{collection_num}"
        async with aiohttp.ClientSession(headers=MeiSpider.get_headers_1()) as session:
            async with session.get(collection_url) as resp:
                if resp.status != 200:
                    print(f"error：获取合集{collection_num}失败，状态码：{resp.status}")
                    return None

                text = await resp.text()
                html = etree.HTML(text)
                tag_names = html.xpath("//div[@class='main-tags']/a/text()")
                total_num = html.xpath("//div[@class='pagenavi']/a[last()-1]/span/text()")[0]
                name = html.xpath("//h2[@class='main-title']/text()")[0]

                img_first_url = html.xpath("//div[@class='main-image']/p/a/img/@src")[0]
                width = html.xpath("//div[@class='main-image']/p/a/img/@width")[0]
                height = html.xpath("//div[@class='main-image']/p/a/img/@height")[0]

                return name, tag_names, total_num, img_first_url, width, height

    @staticmethod
    def get_headers() -> dict:
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

    @staticmethod
    def get_headers_1() -> dict:
        """ 随机返回请求头 """
        headers = [
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
                "Host": "www.mzitu.com",
            },
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
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


if __name__ == "__main__":
    spider = MeiSpider(db_path="///../meizitu.db", dl_cor_num=8)
    # 获取信息到数据库
    spider.save2db()
    # 对于获取合集信息失败的进行补录
    for i in range(1, 4):
        print(f"第{i}次补录开始...")
        spider.re_get_error_collection_to_db()
        print("补录完毕")
    # 将合集下载到本地
    print("稍后将开始下载...")
    spider.save2local(file_path="I:/mmtu")
