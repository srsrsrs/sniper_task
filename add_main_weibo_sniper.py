import redis
from common_config import redis_config, conn_106_mysql
from sqlalchemy import engine, create_engine, func
from table_orm import TWeiboInfo, InfoDataBase  # orm对象,可直接用sqlacodegen生成
from weibo_mobile_spider import *


class WeiboContentListener(object):
    def __init__(self, db):
        self.redis_pool = redis.ConnectionPool(**redis_config)
        self.redis = redis.StrictRedis(connection_pool=self.redis_pool)
        self.db = db

    def get_key_count(self):
        session = self.db.DBSession()
        url_pairs = [(i[0].FstrUrl, i[1]) for i in
                     session.query(TWeiboInfo, func.count(TWeiboInfo.FstrUrl)).group_by(TWeiboInfo.FstrUrl).all()]
        return url_pairs


def main_func_of_spider():
    page_now = 0
    chicken = WeiboOfExxxx()
    max_page = get_max_page(crawl_page_info(chicken.url))
    print(max_page)
    while page_now < max_page:
        print(page_now)
        chicken.pages_add()
        url = chicken.url
        html = crawl_page_info(url)
        row_dict_list = chicken.extract_page_info(html)
        content_list = list(set(i['FstrWeiboContent'] for i in row_dict_list))
        for content in content_list:
            if content == '':
                pass
            if not len(InfoDataBase(conn_106_mysql).DBSession().query(TWeiboInfo).filter(
                            TWeiboInfo.FstrWeiboContent == content).all()):
                insert_row_dict_list = [to_insert_dict for to_insert_dict in row_dict_list if
                                        to_insert_dict['FstrWeiboContent'] == content]
                for row_dict in insert_row_dict_list:
                    count = 0
                    while 1:
                        if count > 10:
                            rds = redis.StrictRedis(**redis_config, db=1)
                            rds.set(hashlib.md5(json.dumps(row_dict).encode()).hexdigest())
                            break
                        try:
                            InfoDataBase(conn_106_mysql).insert_data(row_dict)
                        except OperationalError:
                            time.sleep(3)
                            count += 1
                            continue
                        else:
                            break
        page_now += 1
