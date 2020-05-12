import logging

logger = logging.getLogger("logger")
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter_console = logging.Formatter("%(filename)s-%(levelname)s-%(lineno)s - %(message)s")

console_handler.setFormatter(formatter_console)

logger.addHandler(console_handler)
