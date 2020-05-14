import platform

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Collection, Tag, base, DownloadRecord
from tools import *
from setting import logger

DB_PATH = "///mm.db"


class MMSpider:
    def __init__(self, base_path, ):
        self.num_for_craw = 2  # 网页爬取时并发
        self.num_for_dl = 3  # 图片下载并发

        if not os.path.exists(base_path):
            os.mkdir(base_path)
        self.base_path = base_path  # 图片路径

        self._init_conn_db()
        self.main()

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
        """从妹子图网站同步最新合集数据"""
        new_collection_nums = set(get_all_collection_num())
        old_collection_nums = {i.collection_num for i in self.session.query(DownloadRecord.collection_num).all()}

        update_collection_nums = new_collection_nums - old_collection_nums

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

    def load_not_craw_collection_from_dl_record(self):
        """读取未爬取的合集信息进行爬取"""
        records = self.session.query(DownloadRecord).filter_by(status=0)
        for record in records:
            self.collection_num_queue.put_nowait(record.collection_num)

    def load_not_dl_collection_from_dl_record(self):
        """加载未下载的合集信息"""
        dl_records = self.session.query(DownloadRecord).filter_by(dl_status=0)
        for dl_record in dl_records:
            col_records = self.session.query(Collection).filter_by(collection_num=dl_record.collection_num).all()
            for col in col_records:
                self.collection_info_queue.put_nowait(
                    (col.collection_num, col.name, col.total_num, col.url_prefix, col.url_suffix))

    async def craw_collection(self):
        """爬取合集数据"""
        while True:
            collection_num = await self.collection_num_queue.get()
            res = await get_collection_base_data(collection_num)
            if res is None:
                # 获取合集基本信息失败
                logger.debug(f"获取合集信息失败：{collection_num}")
                self.collection_num_queue.task_done()
                continue

            # 插入合集信息并更新数据库状态，并将信息加入待下载队列中
            self.insert_collection_and_set_craw_status(res)

            await self.collection_info_queue.put(
                (res['collection_num'], res['name'], res['total_num'], res['url_prefix'], res['url_suffix']))

            self.collection_num_queue.task_done()
            logger.debug(f"当前剩余{self.collection_num_queue.qsize()}合集信息获取")

    async def dl_collection(self):
        """按照合集为单位进行下载图片"""
        while True:
            collection_num, name, total_num, url_prefix, url_suffix = await self.collection_info_queue.get()

            url_list = list()
            for i in range(1, int(total_num) + 1):
                num = "0" + str(i) if i < 10 else str(i)
                url = url_prefix + num + url_suffix
                url_list.append(url)

            dl_success = await collection_downloader(self.base_path, name, url_list)
            self.collection_info_queue.task_done()

            if not dl_success:
                continue

            # 合集图片下载完毕后，更新图片下载状态
            self.set_dl_status(collection_num)
            logger.debug(f"合集 {collection_num} 下载完毕")

    def insert_collection_and_set_craw_status(self, res):
        """更新合集信息入库状态"""
        # todo: 判断数据库中是否存在 此合集（编号），若存在，就只更新爬取状态；
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

        self.session.query(DownloadRecord) \
            .filter(DownloadRecord.collection_num == res['collection_num']) \
            .update({"status": 1})

        self.session.commit()
        logger.info(f"合集--{res['name']}--信息入库")

    def set_dl_status(self, collection_num):
        """更新图片下载状态"""
        self.session.query(DownloadRecord) \
            .filter(DownloadRecord.collection_num == collection_num) \
            .update({"dl_status": 1})

        self.session.commit()

    async def _main(self):
        self.collection_info_queue = asyncio.Queue()
        self.collection_num_queue = asyncio.Queue()

        self.sync_from_mzitu()

        self.load_not_craw_collection_from_dl_record()
        logger.debug(f"合集爬虫任务启动，共有{self.collection_num_queue.qsize()}个合集待爬取")

        self.load_not_dl_collection_from_dl_record()
        logger.info(f"共有 {self.collection_info_queue.qsize()} 合集图片需要下载")

        tasks = []
        for _ in range(self.num_for_craw):
            task = asyncio.create_task(self.craw_collection())
            tasks.append(task)

        for _ in range(self.num_for_dl):
            task = asyncio.create_task(self.dl_collection())
            tasks.append(task)

        await self.collection_num_queue.join()
        await self.collection_info_queue.join()

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    def main(self):
        asyncio.run(self._main(), debug=True)


if __name__ == '__main__':
    # todo: 默认下载到磁盘剩余空间最大的盘根目录下的 mzitu 目录
    MMSpider(base_path='D:/mzitu_test')
