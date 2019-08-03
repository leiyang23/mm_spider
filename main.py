import sys, os
from spider.mm_spider import MeiSpider

sys.path.insert(3, os.path.join(os.path.abspath("./"), "db"))

file_path = sys.argv[-1]
if len(sys.argv) == 1:
    print("请输入文件存放路径")
    exit()

spider = MeiSpider()
# 获取信息到数据库
spider.save2db()

# 对于获取信息失败的合集进行补录
for i in range(3):
    print(f"第{i+1}次补录开始...")
    spider.re_get_error_collection_to_db()
    print("补录完毕")

# 将合集下载到本地
print("稍后将开始下载...")
spider.save2local(file_path=file_path)
