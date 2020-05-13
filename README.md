# 妹子图（mzitu.com）增量爬取
## 简介
全站图片爬取，增量更新。  
图片按照专辑保存，保留了图片合集与标签的关联关系。  
主要依赖库：  
- `asyncio`
- `aiohttp`
- `SQLAlchemy`   

**主要特点：** 使用官方的`asyncio`库和第三方`aiohttp`进行异步爬取，可以自定义协程数量来控制爬取速度，
在不影响网站正常运行的前提下学习Python的异步编程。

## 代码
### 表结构    
- `Collection` 合集表，用来记录图片合集的相关信息，是核心数据表。   
- `Tag` 标签表，每个合集关联多个标签。   
- `DownloadRecord` 数据获取记录表，记录合集数据和合集图片的获取记录，增量更新关键。

### 核心逻辑
核心逻辑封装在 spider.py 中的 `MMSpider` 这个类中。          
主要分 3 步。
1. `sync_from_meitu` 用来从网站获取合集编号。    
2. `craw_collection_data_by_dl_record` 根据第一步获取的合集编号爬取合集信息并入库。    
3. `dl_img_by_dl_record` 根据第二步获取的合集信息进行下载图片。  

这 3 步既可以按顺序执行，也可以单独执行，并没有强依赖关系。之间是通过数据库进行连接。      

## 使用   
首先根据`requirements.txt`配置环境：` pip install -r requirements.txt` 
源码下 `python spider.py `    

由于没有配置代理IP等反爬措施，爬取速度设置的比较慢，网页爬取
默认爬取主站网页开启 2 个协程，下载图片默认开启 2 个协程，
一般1-2h就可以完成（大概15GB）。之后运行会自动比对数据库数据和网站数据，进行增量更新。

> 默认程序会检测剩余磁盘容量最大的盘根路径下进行存放图片，默认目录名称 `mm_img`。   

> 严正声明：仅供学习交流，请勿对网站正常运行造成影响。


