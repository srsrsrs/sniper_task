import re
import time
from urllib import request
import requests

import pymysql
import requests
import ssl
import http.cookiejar
import pandas as pd
import time
import numpy as np
from sqlalchemy import create_engine, engine
from sqlalchemy.exc import OperationalError
import random
import hashlib
from common_config import cookie_weibo_mobile, uid, conn_106_mysql, redis_config
from table_orm import TWeiboInfo, InfoDataBase, metadata
import redis
import json

# cookie_jar = requests.utils.cookiejar_from_dict(cookies, cookiejar=None, overwrite=True)
# opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))

ssl._create_default_https_context: ssl._create_unverified_context
uid = int(str(uid)[6:])


# 一开始在common_config里面配置的是sina PC端UID,移动端UID为PC端UID6位以后数字

def re_match(html, qual):
    quallist = re.findall(qual, html)
    return quallist


class WeiboOfExxxx(object):
    def __init__(self):
        self.pages = 0
        self.url = "https://weibo.cn/u/{}?filter=0&page=0".format(uid)
        self.weibo_count = 0

    def pages_add(self):
        self.pages += 1
        self.url = """https://weibo.cn/u/{}?filter=0&page={}""".format(uid, self.pages)

    def extract_page_info(self, page_html):
        row_dict_list = extract_info_from_page_html(page_html)
        if not row_dict_list:
            return None
        for row_dict in row_dict_list:
            row_dict['FuiWeiboId'] = self.weibo_count
            row_dict['FstrPageUrl'] = self.url
            row_dict['FuiIfDelete'] = 0
        self.weibo_count += 1
        return row_dict_list


def extract_info_from_page_html(html):
    text_list = re_match(html, r'<div><span class="ctt">.*?</span></div></div><div class="s">')
    if len(text_list) == 0:  # 即当页全为转发,留待未来处理
        return None
    row_dict_list = []
    for single_text in text_list:
        weibo_content, weibo_create_time = get_weibo_text(single_text)
        comment_count, comment_url = get_comment_info(single_text)
        row_dict = {}
        if comment_count != 0:
            comment = comment_pagely_craw(comment_url)
            for row in range(comment_count):
                if row > len(comment['FuiCommentId']) - 1:
                    print("{} has a wrong comment count,maybe they delete some comments".format(comment_url))
                    # 有时微博显示的评论条数会比实际抓取到的多,猜测可能是由于用户删除了评论,不管以后是否处理,在此先留下记录.
                    continue
                for key in comment.keys():
                    row_dict[key] = comment[key][row]
                row_dict['FuiWeiboCt'] = time_transfer(weibo_create_time)
                row_dict['FstrWeiboContent'] = weibo_content
                row_dict['FstrUrl'] = comment_url
                row_dict['FuiCommentCount'] = comment_count
                row_dict['FstrWeiboContentHash'] = hashlib.md5(row_dict['FstrWeiboContent'].encode()).hexdigest()
                row_dict['FstrCommentContentHash'] = hashlib.md5(
                    row_dict['FstrCommentContent'].encode()).hexdigest()
                row_dict_list.append(row_dict.copy())  # list append dict 事实上是对dict的浅拷贝,所以每当遍历到新一行,旧的一行被更新,导致数据重复插入
        else:
            """part_df = pd.DataFrame(
                data=[[comment_url, weibo_count, weibo_content, time_transfer(weibo_create_time), None, None, None,
                       None, None, None]],
                columns=['FstrUrl', 'FuiWeiboId', 'FstrWeiboContent', 'FuiWeiboCt', 'FuiCommentId',
                         'FstrCommentMaker', 'FstrReplyTo', 'FstrCommentContent', 'FuiCommentCt',
                         'FuiReplyType']
                , index=range(1))"""
            row_dict['FstrCommentContent'] = None
            row_dict['FstrCommentMaker'] = None
            row_dict['FstrReplyTo'] = None
            row_dict['FuiCommentCt'] = None
            row_dict['FuiCommentId'] = None
            row_dict['FuiReplyType'] = None
            row_dict['FuiWeiboCt'] = time_transfer(weibo_create_time)
            row_dict['FstrWeiboContent'] = weibo_content
            row_dict['FstrUrl'] = comment_url
            row_dict['FuiCommentCount'] = comment_count
            row_dict['FstrWeiboContentHash'] = hashlib.md5(row_dict['FstrWeiboContent'].encode()).hexdigest()
            if row_dict['FstrCommentContent']:
                row_dict['FstrCommentContentHash'] = hashlib.md5(
                    row_dict['FstrCommentContent'].encode()).hexdigest()
            else:
                row_dict['FstrCommentContentHash'] = None
            row_dict_list.append(row_dict)
    return row_dict_list


def crawl_page_info(url):
    count = 0
    url_text = None
    while 1:
        count += 1
        if count > 20:
            break
        try:
            m = random.randint(0, 100 * count) / 100 + 1
            # print(m)
            time.sleep(m)
            r = requests.get(url, cookies=cookie_weibo_mobile)
            r.headers = {'User-Agent': 'Mozilla/5.0(Windows NT 6.1;WOW64;rv:47.0) Gecko/20100101 Firefox/47.0'}
            break
        except:
            print('time out and error page is ' + str(url))
            continue
        finally:
            url_text = r.text
            if url_text == '':
                print("page is temporarily unavailable, we'll sleep about 5 minutes.")
                time.sleep(300)
    return url_text


def get_weibo_text(single_text):
    texts = re_match(single_text, r'<div><span class="ctt">(.*?)</span>')
    weibo_create_time = re_match(single_text, r'<span class="ct">(.*?)&nbsp;')[0]
    if len(texts) != 0:
        texts = texts[0]
    else:
        return None
    labels = re_match(texts, r'<.*?>')
    for i in labels:
        texts = texts.replace(i, '')
    return texts, weibo_create_time


def get_comment_info(single_text):
    text = re_match(single_text, r'<a href="https://weibo.cn/comment/.*?]')
    if len(text) != 0:
        text = text[0]
    else:
        return 0, ''
    comment_count = int(re_match(text, '\[(.*?)\]')[0])
    comment_url = re_match(text, r'<a href="(.*?)" class="cc">')[0]
    return comment_count, comment_url


def get_max_page(html):
    max_page = int(re_match(html, r'<input type="submit" value="跳页" />&nbsp;[1-9]*/(.*?)页</div></form></div>')[0])
    return max_page


def delete_html_info(raw_str):
    html_info = re_match(raw_str, r'<.*?>')
    for i in html_info:
        raw_str = raw_str.replace(i, '')
    return raw_str


def time_transfer(raw_str):
    if "今天" in raw_str:
        raw_str = time.strftime('%m%d') + raw_str.strip('今天')
    elif "分钟前" in raw_str:
        raw_str = time.mktime() - int(raw_str.strip("分钟前")) * 60

    try:
        transfered_time = int(time.mktime(time.strptime(raw_str, '%Y-%m-%d %H:%M:%S')))
    except:
        raw_str = raw_str.replace('月', '')
        raw_str = raw_str.replace('日', '')
        raw_str = time.strftime('%Y') + raw_str
        transfered_time = int(time.mktime(time.strptime(raw_str, '%Y%m%d %H:%M')))
    return transfered_time


def get_comment_detail(text):
    comment_info = {'FuiCommentId': [],
                    'FstrCommentMaker': [],
                    'FstrReplyTo': [],
                    'FstrCommentContent': [],
                    'FuiCommentCt': [],
                    'FuiReplyType': []}

    text = re_match(text, r'<input type="submit" value="评论".*')
    if len(text) != 0:
        text = text[0]
    else:
        return comment_info
    text_list = re_match(text, r'<a href="/.*?</span></div>')
    j = 0
    for i in text_list:
        reply_type = 0  # 非回复
        comment_maker = re_match(i, r'<a href=".*?">(.*?)</a>')[0]
        comment_body = re_match(i, r'<span class="ctt">(.*?)</span>')[0]
        reply_to = re_match(comment_body, r'回复<a href=".*?">@(.*?)</a>')
        if len(reply_to) == 0:
            reply_to = ''
            comment_content = delete_html_info(comment_body)
        else:
            reply_to = reply_to[0]
            try:
                comment_content = delete_html_info(re_match(comment_body, r'</a>[:,：](.*)')[0])
                reply_type = 1  # 正常回复
            except:
                try:
                    comment_content = delete_html_info(re_match(comment_body, r'</a> 的表态:(.*)')[0])
                    reply_type = 1  # 正常回复
                except:
                    comment_content = delete_html_info(re_match(comment_body, r'</a> 的赞:(.*)')[0])
                    reply_type = 2  # 回复赞
        ct_html = re_match(i, r'<span class="ct">(.*?)&nbsp;')[0]
        comment_ct = time_transfer(ct_html)

        comment_info['FuiCommentId'].append(j)
        comment_info['FstrCommentMaker'].append(comment_maker)
        comment_info['FstrReplyTo'].append(reply_to)
        comment_info['FstrCommentContent'].append(comment_content)
        comment_info['FuiCommentCt'].append(comment_ct)
        comment_info['FuiReplyType'].append(reply_type)
        j += 1
    return comment_info


def comment_pagely_craw(url):
    comment_detail = crawl_page_info(url)
    comment_max_page = re_match(comment_detail, r'<input type="submit" value="跳页" />&nbsp;.*?/(.*?)页')
    if len(comment_max_page) != 0:
        comment_max_page = int(comment_max_page[0])
        weibo_comment_detail = get_comment_detail(comment_detail)
        for comment_pages in range(comment_max_page - 1):
            page = comment_pages + 2
            url_new = re_match(url, r'(.*?)#')[0]
            url_new = url_new + '&page={}'.format(page)
            page_detail = get_comment_detail(crawl_page_info(url_new))
            for i in weibo_comment_detail.keys():
                weibo_comment_detail[i].extend(page_detail[i])
    else:
        weibo_comment_detail = get_comment_detail(comment_detail)
    return weibo_comment_detail


def try_create_table():
    db = InfoDataBase(conn_106_mysql)
    try:
        len(db.DBSession().query(TWeiboInfo).all())
    except Exception as e:
        print(e)
        print("\nAnd Table has already be created")
        metadata.create_all(db.engine)


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
        if row_dict_list:
            pass
        else:
            print("page {} is all forwarding, and link is {}".format(page_now, chicken.url))
            page_now += 1  # 当页全为转发,暂时对转发内容没有兴趣
            continue
        for row_dict in row_dict_list:
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


def insert_from_redis():
    rds = redis.StrictRedis(**redis_config, db=1)
    keys = rds.keys()
    for key in keys:
        try:
            InfoDataBase(conn_106_mysql).insert_data(json.loads(rds.get(key)))
        except OperationalError:
            print(json.loads(rds.get(key)))
            pass
        else:
            rds.delete(key)


if __name__ == '__main__':
    redis.StrictRedis(**redis_config, db=1).flushdb()
    try_create_table()
    main_func_of_spider()
    insert_from_redis()
