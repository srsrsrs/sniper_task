cookie_weibo_mobile = {
    'SCF': '',
    '_T_WM': '',
    'SUB': '',
    'SUHB': '',
    'WEIBOCN_FROM': '',
    'MLOGIN': '',
    'M_WEIBOCN_PARAMS': ''}

uid = 1234567890

conn_106_mysql = {
    "drivername": "mysql+pymysql",
    "host": '${host}',
    "port": ${port},
    "username": '${username}',
    "database": '${database}',
    "password": '${password}',
    "query": {"charset": "utf8mb4"}
}

redis_config = {'host': '${host}', 'port': ${port}}

mail_config = {'user': '${user}',
               'password': '${password}', # 注意,此处的密码不是邮箱密码,而是SMTP的授权码
               'host': '${host}', # 比如smtp.qq.com
               'port': '${port}'} # smtp.qq.com的端口是456
receiver = "$receiver"
