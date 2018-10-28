import redis
from common_config import redis_config, conn_106_mysql
from sqlalchemy import engine, create_engine, func
from table_orm import TWeiboInfo, InfoDataBase  # orm对象,可直接用sqlacodegen生成
from weibo_mobile_spider import *
from comm_func import send_mail


class WeiboCommentListener(object):
    def __init__(self, db):
        self.redis_pool = redis.ConnectionPool(**redis_config)
        self.redis = redis.StrictRedis(connection_pool=self.redis_pool)
        self.db = db

    def get_key_count(self):
        session = self.db.DBSession()
        url_pairs = [(i[0].FstrUrl, i[1]) for i in
                     session.query(TWeiboInfo, func.count(TWeiboInfo.FstrUrl)).group_by(TWeiboInfo.FstrUrl).order_by(TWeiboInfo.FuiWeiboCt.desc()).all()]
        return url_pairs


if __name__ == '__main__':
    report_data_consumer = WeiboCommentListener(InfoDataBase(conn_106_mysql))
    key_pairs = report_data_consumer.get_key_count()
    while 24 > int(time.strftime("%H")) > 5:
        for key in key_pairs:
            print(key)
            new_comment = comment_pagely_craw(key[0])
            if len(new_comment['FstrCommentContent']) == key[1]:
                continue
            else:
                old_info = report_data_consumer.db.DBSession().query(TWeiboInfo).filter(
                    TWeiboInfo.FstrUrl == key[0]).all()
                old_comment = [info.FstrCommentContent for info in old_info]
                if len(new_comment['FstrCommentContent']) > key[1]:
                    for i in range(len(new_comment['FstrCommentContent'])):
                        if new_comment['FstrCommentContent'][i] not in old_comment:
                            insert_dict = {key: new_comment[key][i] for key in new_comment.keys()}
                            for old_info_key in old_info[0].__dict__.keys():
                                if old_info_key != 'FuiId' and old_info_key != '_sa_instance_state' and old_info_key not in insert_dict.keys():
                                    insert_dict[old_info_key] = old_info[0].__getattribute__(old_info_key)
                            session = report_data_consumer.db.DBSession()
                            session.add(TWeiboInfo(**insert_dict))
                            session.commit()
                            session.close()
                            send_mail(subject="微博'{}'发现[{}]的新评论,内容为'{}'".format(insert_dict['FstrWeiboContent'],
                                                                                insert_dict['FstrCommentMaker'],
                                                                                insert_dict['FstrCommentContent']),
                                      content=crawl_page_info(key[0]))
                            print(insert_dict)
            time.sleep(random.randint(0, 100) / 100 + 1)
        time.sleep(120)
