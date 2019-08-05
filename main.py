import sys
import logging

from spider.mm_spider import MeiSpider

# 配置日志模块
logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("err_log.log")
file_handler.setLevel(logging.ERROR)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter_console = logging.Formatter("%(levelname)s - %(lineno)s - %(message)s")
formatter_file = logging.Formatter("%(asctime)s - %(levelname)s - %(lineno)s - %(message)s")
console_handler.setFormatter(formatter_console)
file_handler.setFormatter(formatter_file)

logger.addHandler(console_handler)

DB_PATH = "///./meizitu.db"

file_path = sys.argv[-1]
if len(sys.argv) == 1:
    logger.error("请输入文件存放路径")
    exit()

spider = MeiSpider(db_path=DB_PATH)
# 获取信息到数据库
spider.save2db()

# 对于获取信息失败的合集进行补录
for i in range(3):
    logger.info(f"第{i + 1}次补录开始...")
    spider.re_get_error_collection_to_db()
    logger.info("补录完毕")

# 将合集下载到本地
logger.info("稍后将开始下载...")
spider.save2local(file_path=file_path)
