from sqlalchemy import create_engine
import ssl
import requests
import time
import re
import pandas as pd
from common_config import cookie_weibo_com,uid


ssl._create_default_https_context: ssl._create_unverified_context


def crawl_page_info(url):
    count = 0
    while 1:
        count += 1
        if count > 3:
            break
        try:
            r = requests.get(url, cookies=cookie_weibo_com)
            r.headers = {'User-Agent': 'Mozilla/5.0(Windows NT 6.1;WOW64;rv:47.0) Gecko/20100101 Firefox/47.0'}
            time.sleep(1)
        except:
            print('time out and error page is ' + str(url))
            continue
    return r.text


def re_match(html, qual):
    quallist = re.findall(qual, html)
    return quallist


def get_monthly_url(month, page):
    url = "http://weibo.com/p/{}/home?is_all=1&stat_date={}&page={}#feedtop".format(uid,month,page)
    return url


def get_first_month_info(text):
    first_weibo_month = re_match(re_match(text, r'<li class=\\"last\\">(.*?)第一条微博')[0], r'stat_date=(.*?)&page')[0]
    return first_weibo_month


def generate_moth_range(first_weibo_month, month_now):
    range_df = pd.DataFrame(pd.date_range(start=first_weibo_month + '01', end=month_now + '01', freq="1m"))
    range_df = range_df.applymap(lambda x: str(x)[:7].replace("-", ""))
    return range_df


def delete_html_info(raw_str):
    html_info = re_match(raw_str, r'<.*?>')
    for i in html_info:
        raw_str = raw_str.replace(i, '')
    return raw_str


def main_craw_func():
    month_now = time.strftime("%Y%m")
    url = "http://weibo.com/p/{}/home?is_all=1&stat_date={}#feedtop".format(uid,month_now)
    text = crawl_page_info(url)
    user_name = re_match(text, r'，([^\n]*?)的微博')[0]
    first_weibo_month = get_first_month_info(text)
    range_df = generate_moth_range(first_weibo_month, month_now)
    lines = 0
    old_content = []  # 懒得找当月微博有多少页,直接比较下一页内容是否与之前相同,相同就说明抓完了
    while 1:
        try:
            month = range_df.iloc[lines, 0]
        except IndexError:
            break
        page = 1
        while 1:
            page_url = get_monthly_url(month, page)
            page_text = crawl_page_info(page_url)
            weibos = re_match(page_text, r'nick-name=\\"{}\\">([^\n]*?)<\\/div>'.format(user_name))
            if weibos == old_content:
                print("skipped and time is {} and page is {}".format(month, page))
                break
            else:
                old_content = weibos
            for i in weibos:
                content = i.replace('\\n', '').replace(" ", "")
                print(content)
                file = open("/home/yunshu.zhou/Weibo.txt", "a")
                file.write(content+'\n')
                file.close()
            page += 1
        lines += 1

if __name__ == "__main__":
    main_craw_func()
