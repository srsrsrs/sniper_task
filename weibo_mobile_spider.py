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
from sqlalchemy import create_engine,engine
import random
from common_config import cookie_weibo_mobile,uid,conn_106_mysql


# cookie_jar = requests.utils.cookiejar_from_dict(cookies, cookiejar=None, overwrite=True)
# opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))

ssl._create_default_https_context: ssl._create_unverified_context
uid = int(str(uid)[6:])


def re_match(html, qual):
    quallist = re.findall(qual, html)
    return quallist


class WeiboOfExxxx(object):
    def __init__(self):
        self.pages = 0
        self.url = "https://weibo.cn/u/{}?filter=1&page=0".format(uid)

    def pages_add(self):
        self.pages += 1
        self.url = """https://weibo.cn/u/{}?filter=1&page={}""".format(uid,self.pages)
        bak_uid = 'https://weibo.cn/felonerroks'


def crawl_page_info(url):
    count = 0
    while 1:
        count += 1
        if count > 20:
            break
        try:
            m = random.randint(0, 100 * count) / 100
            print(m)
            time.sleep(m)
            r = requests.get(url, cookies=cookie_weibo_mobile)
            r.headers = {'User-Agent': 'Mozilla/5.0(Windows NT 6.1;WOW64;rv:47.0) Gecko/20100101 Firefox/47.0'}
            break
        except:
            print('time out and error page is ' + str(url))
            continue
    return r.text


def get_weibo_text(single_text):
    texts = re_match(single_text, r'<div><span class="ctt">(.*?)</span>')
    if len(texts) != 0:
        texts = texts[0]
    else:
        return None
    labels = re_match(texts, r'<.*?>')
    for i in labels:
        texts = texts.replace(i, '')
    return texts


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
    max_page = int(re_match(html, r'<input type="submit" value="跳页" />&nbsp;1/(.*?)页</div></form></div>')[0])
    return max_page


def delete_html_info(raw_str):
    html_info = re_match(raw_str, r'<.*?>')
    for i in html_info:
        raw_str = raw_str.replace(i, '')
    return raw_str


def time_transfer(raw_str):
    try:
        transfered_time = int(time.mktime(time.strptime(raw_str, '%Y-%m-%d %H:%M:%S')))
    except:
        raw_str = raw_str.replace('月', '')
        raw_str = raw_str.replace('日', '')
        raw_str = time.strftime('%Y') + raw_str
        transfered_time = int(time.mktime(time.strptime(raw_str, '%Y%m%d %H:%M')))
    return transfered_time


def get_comment_detail(text):
    text = re_match(text, r'<input type="submit" value="评论".*')
    if len(text) != 0:
        text = text[0]
    else:
        comment_info_df = pd.DataFrame()
        return comment_info_df
    text_list = re_match(text, r'<a href="/.*?</span></div>')
    comment_info_df = pd.DataFrame()
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
        comment_ct = time_transfer(re_match(i, r'<span class="ct">(.*?)&nbsp;')[0])

        comment_info = {'FuiCommentId': j,
                        'FstrCommentMaker': comment_maker,
                        'FstrReplyTo': reply_to,
                        'FstrCommentContent': comment_content,
                        'FuiCommentCt': comment_ct,
                        'FuiReplyType': reply_type}
        comment_info_df = pd.concat([comment_info_df, pd.DataFrame(comment_info, index=range(1))], axis=0)
        comment_info_df = comment_info_df.loc[:, comment_info.keys()]
        comment_info_df['FuiCommentId'] = comment_info_df['FuiCommentId'].sort_values(ascending=False)
        j += 1
    return comment_info_df


def comment_pagely_craw(url):
    comment_detail = crawl_page_info(url)
    comment_max_page = re_match(comment_detail, r'<input type="submit" value="跳页" />&nbsp;.*?/(.*?)页')
    if len(comment_max_page) != 0:
        comment_max_page = int(comment_max_page[0])
        part_df = get_comment_detail(comment_detail)
        for comment_pages in range(comment_max_page - 1):
            page = comment_pages + 1
            url_new = re_match(url, r'(.*?)#')[0]
            url_new = url_new + '&page={}'.format(page)
            page_detail = crawl_page_info(url_new)
            part_df = pd.concat([part_df, get_comment_detail(page_detail)], axis=0)
    else:
        part_df = get_comment_detail(comment_detail)
    return part_df


def main_func_of_spider():
    max_page = 1
    page_now = 0
    chicken = WeiboOfExxxx()
    weibo_count = 0
    while page_now < max_page:
        chicken.pages_add()
        url = chicken.url
        html = crawl_page_info(url)
        text_list = re_match(html, r'<div><span class="ctt">.*?</span></div></div><div class="s">')
        main_df = pd.DataFrame()
        try:
            max_page = get_max_page(html)
        except:
            max_page = max_page
        for single_text in text_list:
            weibo_content = get_weibo_text(single_text)
            comment_count, comment_url = get_comment_info(single_text)
            if comment_count != 0:
                part_df = comment_pagely_craw(comment_url)
                part_df.insert(loc=0, column='FstrWeiboContent', value=weibo_content)
                part_df.insert(loc=0, column='FuiWeiboId', value=weibo_count)
                part_df.insert(loc=0, column='FstrUrl', value=comment_url)
                weibo_count += 1
                main_df = pd.concat([main_df, part_df], axis=0)
            else:
                part_df = pd.DataFrame(
                    data=[[None, None, None, weibo_content, None, None, None, weibo_count, comment_url]],
                    columns=['FstrCommentContent', 'FstrCommentMaker', 'FstrReplyTo',
                             'FstrWeiboContent', 'FuiCommentCt', 'FuiReplyType', 'FuiCommentId',
                             'FuiWeiboId', 'FstrUrl'], index=range(1))
                weibo_count += 1
                main_df = pd.concat([main_df, part_df], axis=0)
        #print(main_df)
        main_df.FstrWeiboContent = main_df.FstrWeiboContent.astype(str)
        local_conn = create_engine(engine.url.URL(**conn_106_mysql))
        main_df.fillna(0).to_sql('t_weibo_info', local_conn, if_exists='append', index=False)
        page_now += 1


if __name__ == '__main__':
    main_func_of_spider()

