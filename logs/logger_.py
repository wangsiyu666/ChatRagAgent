import logging
import os
import langchain
import tempfile
import shutil


# 是否显示详细日志
log_verbose = False
langchain.verbose = False


#日志格式
LOG_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format=LOG_FORMAT)

#日志存储路径
LOG_PATH = "E:\公司\copilotkit\logs\log"
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)


