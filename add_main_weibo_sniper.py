import redis
from common_config import redis_config, conn_106_mysql
from sqlalchemy import engine, create_engine, func
from table_orm import TWeiboInfo, InfoDataBase  # orm对象,可直接用sqlacodegen生成
from weibo_com_spider import *


class WeiboListener(object):
    def __init__(self, db):
        self.redis_pool = redis.ConnectionPool(**redis_config)
        self.redis = redis.StrictRedis(connection_pool=self.redis_pool)
        self.db = db

    def insert_data(self):
        session = self.db.DBSession()
        url_pairs = [(i[0].FstrUrl, i[1]) for i in
                     session.query(TWeiboInfo, func.count(TWeiboInfo.FstrUrl)).group_by(TWeiboInfo.FstrUrl).all()]
        return url_pairs


if __name__ == '__main__':
    report_data_consumer = WeiboListener(InfoDataBase(conn_106_mysql))
