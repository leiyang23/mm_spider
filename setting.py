import logging

debug = True
# windows 下文件（夹）命名中的非法字符
invalid_chars_in_path = ['*', '|', ':', '：', '?', '/', '<', '>', '"', '\\']

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
    "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)"
]

logger = logging.getLogger("logger")
if debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter_console = logging.Formatter("%(asctime)s-%(filename)s-%(levelname)s-%(lineno)s - %(message)s",
                                      datefmt='%Y-%m-%d %H:%M:%S')

console_handler.setFormatter(formatter_console)

logger.addHandler(console_handler)
