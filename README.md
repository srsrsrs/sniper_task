# sniper_task
<pre>一个简单的爬虫,用于抓取单一某个用户的全部原创wb内容及其评论</pre>
**

### 要求
* Python : 3.6+
* Mysql
* Redis
* 已有cookie

请将相关连接信息配置入common_config中

### 主要脚本功能
* weibo\_mobile\_spider.py : 全量抓取脚本,用于第一次全量抓取内容
* add\_main\_weibo\_sniper.py : 监听wb内容常驻脚本
* add\_comment\_sniper.py : 监听已有wb评论内容常驻脚本

