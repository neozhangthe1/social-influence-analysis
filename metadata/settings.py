DB_HOST = "10.1.1.110"
DB_USER = "root"
DB_PORT = 3306
DB_PASS = "keg2012"
DB_NAME = "arnet_db"


MONGO_HOST = "localhost"
MONGO_NAME = "scrapy"

MONGO_HOST61 = "10.1.1.61"

import os
HERE = os.path.abspath(os.curdir)
PROJ_PATH = os.path.split(os.path.split(HERE)[0])[0]
DATA_PATH = os.path.join(PROJ_PATH, "data").replace('\\','/')
LOG_PATH = ""
