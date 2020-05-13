from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Collection, Tag, base, DownloadRecord
from tools import *
from setting import logger

DB_PATH = "///mm.db"


class MMSpider:
    def __init__(self, base_path, ):
        self.concurrent_num = 2
        self.dl_num = 2
        self.base_path = base_path
        self._init_conn_db()

    def _init_conn_db(self):
        """初始化连接数据库"""
        try:
            engine = create_engine(f"sqlite:{DB_PATH}", echo=False)
            db_session = sessionmaker(bind=engine)
            self.session = db_session()
            # 创建表（如果表已经存在，则不会创建）
            base.metadata.create_all(engine)
            logger.info("初始化连接数据库完毕")
        except ImportError as e:
            if e.name == '_sqlite3':
                logger.error(f"执行 yum install sqlite-devel，之后重新编译安装Python")
            else:
                logger.error(f"请检查是否安装此模块：{e.name}")
            exit()

    def _inset_collection(self, res):
        # 合集信息入库
        logger.debug(f"入库合集信息：{res}")

        tags = []
        for tag_name in res['tag_names']:
            t = self.session.query(Tag).filter_by(tag_name=tag_name)
            if t.count() == 0:  # tag不存在
                self.session.add(Tag(tag_name=tag_name))
                self.session.commit()
                tags.append(self.session.query(Tag).filter_by(tag_name=tag_name).first())
            else:  # tag 存在
                tags.append(t.first())

        del res['tag_names']
        res['tags'] = tags
        collection = Collection(**res)
        self.session.add(collection)

        self.session.query(DownloadRecord).filter(DownloadRecord.collection_num == res['collection_num']).update(
            {"status": 1})

        self.session.commit()
        logger.debug(f"合集信息入库：{res}")

    def update_dl_record(self):
        # 同步最新合集数据
        new_collection_nums = set(get_all_collection_num())
        old_collection_nums = {i.collection_num for i in self.session.query(DownloadRecord.collection_num).all()}

        update_collection_nums = new_collection_nums - old_collection_nums
        logger.debug("新增数据：")
        logger.debug(update_collection_nums)

        count = 0
        for collection_num in update_collection_nums:
            # 格式校验
            if not collection_num.isdigit():
                continue

            self.session.add(DownloadRecord(collection_num=collection_num))
            count += 1
            if count % 100 == 0:
                self.session.flush()
        self.session.commit()
        logger.info(f"下载数据更新，新增{count}条合集数据")

    async def update_collection_record(self):
        # 爬取合集数据
        collection_num_queue = asyncio.Queue()

        records = self.session.query(DownloadRecord).filter_by(status=0)
        for record in records:
            collection_num_queue.put_nowait(record.collection_num)
        logger.debug(f"待下载合集准备完毕")

        tasks = []
        for _ in range(self.concurrent_num):
            task = asyncio.create_task(self.collection_worker(collection_num_queue))
            tasks.append(task)
        logger.debug("合集爬虫任务启动")

        await collection_num_queue.join()

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("合集信息已全部写入数据库")

    async def collection_worker(self, num_queue: asyncio.Queue):
        session = aiohttp.ClientSession(headers=get_headers_1())
        while True:
            collection_num = await num_queue.get()
            logger.debug(f"{collection_num}开始。。")
            res = await get_collection_base_data(session, collection_num)
            if res is None:  # 获取合集基本信息失败
                logger.debug(f"获取合集信息失败：{collection_num}")
                num_queue.task_done()
                continue
            self._inset_collection(res)
            logger.debug(f"{collection_num}结束。。")
            num_queue.task_done()
            if num_queue.empty():
                await session.close()

    async def update_files(self):
        # 同步本地图片
        dl_info_queue = asyncio.Queue()

        dl_records = self.session.query(DownloadRecord).filter_by(dl_status=0)
        for dl_record in dl_records:
            col_records = self.session.query(Collection).filter_by(collection_num=dl_record.collection_num).all()
            for col in col_records:
                for i in range(1, int(col.total_num)):
                    num = "0" + str(i) if i < 10 else str(i)
                    url = col.url_prefix + num + col.url_suffix
                    dl_info_queue.put_nowait((col.name, url))
            dl_record.dl_status = 1
            self.session.commit()

        logger.debug(f"共有{dl_info_queue.qsize()}图片需要下载")

        tasks = []
        for _ in range(self.dl_num):
            task = asyncio.create_task(download_worker(self.base_path, dl_info_queue))
            tasks.append(task)

        await dl_info_queue.join()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def main(self):
        self.update_dl_record()
        asyncio.run(self.update_collection_record())
        asyncio.run(self.update_files())


if __name__ == '__main__':
    import time

    count = 0
    while True:
        try:
            MMSpider(base_path='D:/mzitu').main()
        except TimeoutError:
            count += 1
            logger.warning(f"第 {count} 次被禁，20分钟后再试")
            time.sleep(60 * 20)
            continue
