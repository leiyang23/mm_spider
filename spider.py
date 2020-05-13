from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Collection, Tag, base, DownloadRecord
from tools import *
from setting import logger

DB_PATH = "///mm.db"


class MMSpider:
    def __init__(self, base_path, ):
        self.num_for_craw = 2  # 网页爬取时并发
        self.num_for_dl = 2  # 图片下载并发

        if not os.path.exists(base_path):
            os.mkdir(base_path)
        self.base_path = base_path  # 图片路径

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

    def sync_from_mzitu(self):
        # 从妹子图网站同步最新合集数据
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

    async def craw_collection_data_by_dl_record(self):
        # 读取未爬取的合集信息进行爬取
        collection_num_queue = asyncio.Queue()

        records = self.session.query(DownloadRecord).filter_by(status=0)
        for record in records:
            collection_num_queue.put_nowait(record.collection_num)
        logger.debug(f"待下载合集准备完毕")

        tasks = []
        for _ in range(self.num_for_craw):
            task = asyncio.create_task(self.craw_collection_data(collection_num_queue))
            tasks.append(task)
        logger.debug("合集爬虫任务启动")

        await collection_num_queue.join()

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("合集信息已全部写入数据库")

    async def dl_img_by_dl_record(self):
        # 下载图片
        collection_info_queue = asyncio.Queue()

        dl_records = self.session.query(DownloadRecord).filter_by(dl_status=0)
        for dl_record in dl_records:
            col_records = self.session.query(Collection).filter_by(collection_num=dl_record.collection_num).all()
            for col in col_records:
                collection_info_queue.put_nowait(
                    (col.collection_num, col.name, col.total_num, col.url_prefix, col.url_suffix))

        tasks = []
        for _ in range(self.num_for_dl):
            task = asyncio.create_task(self.dl_collection_img(collection_info_queue))
            tasks.append(task)

        await collection_info_queue.join()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=False)

    async def craw_collection_data(self, num_queue: asyncio.Queue):
        """爬取合集数据"""
        while True:
            collection_num = await num_queue.get()
            logger.debug(f"{collection_num}开始。。")
            res = await get_collection_base_data(collection_num)
            if res is None:  # 获取合集基本信息失败
                logger.debug(f"获取合集信息失败：{collection_num}")
                num_queue.task_done()
                continue

            self.update_collection_status(res)
            logger.debug(f"{collection_num}结束。。")

            num_queue.task_done()

    async def dl_collection_img(self, q: asyncio.Queue):
        """按照合集为单位进行下载图片"""
        while True:
            collection_num, name, total_num, url_prefix, url_suffix = await q.get()

            url_list = list()
            for i in range(1, int(total_num)+1):
                num = "0" + str(i) if i < 10 else str(i)
                url = url_prefix + num + url_suffix
                url_list.append(url)

            await download_worker(self.base_path, name, url_list)

            # 合集图片下载完毕后，更新图片下载状态
            self.update_dl_status(collection_num)

    def update_collection_status(self, res):
        """更新合集信息入库状态"""
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

    def update_dl_status(self, collection_num):
        """更新图片下载状态"""
        self.session.query(DownloadRecord).filter(DownloadRecord.collection_num == collection_num).update(
            {"dl_status": 1})
        self.session.commit()

    def main(self):
        # self.sync_from_mzitu()
        # asyncio.run(self.craw_collection_data_by_dl_record())
        asyncio.run(self.dl_img_by_dl_record())


if __name__ == '__main__':
    # todo: 默认下载到磁盘剩余空间最大的盘根目录下的 mzitu 目录
    MMSpider(base_path='D:/mzitu_test').main()
