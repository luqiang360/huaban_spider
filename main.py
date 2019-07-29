#!/usr/local/data/anaconda2/bin/python
# -*- coding: utf-8 -*-
import pymysql                          # pymysql
from optparse import OptionParser       # OptionParser

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def mysql_create(db, key, value):
    # 连接MySQL数据库
    conn = pymysql.connect(host=opt.ip, user=opt.usr, 
                passwd=opt.pwd, charset='utf8')
    # MySQL查询语句
    sql = 'CREATE TABLE IF NOT EXISTS {} ('.format(db) + ', '.join(
            map(lambda kv : '`{}` {}'.format(*kv), zip(key , 
            value))) + ', PRIMARY KEY (`{}`))'.format(key[0])

    try:
        with conn.cursor() as cursor:
            # 执行MySQL查询语句
            cursor.execute(sql)

        # 提交MySQL数据库
        conn.commit()
    finally:
        # 关闭MySQL数据库
        conn.close()

def mysql_create_board(opt):
    # 用户数据库
    db = '`huaban2`.`board.{}.{}`'.format(opt.cat, opt.db)
    #  键key
    key = ['board_id', 'user_id', 'title', 'description', 
            'category_id', 'pin_count', 'follow_count', 
            'like_count', 'created_at', 'updated_at', 
            'category_name', 'isMuseBoard', 'crawl_at']
    # 值value
    value = ['INT NOT NULL', 'INT', 'TEXT', 'TEXT', 
                'TEXT', 'INT', 'INT', 'INT', 'INT', 
                'INT', 'TEXT', 'BOOL', 'INT']

    # 调用MySQL新建函数
    mysql_create(db, key, value)

def mysql_create_user(opt):
    # 用户数据库
    db = '`huaban2`.`user.{}.{}`'.format(opt.cat, opt.db)
    # 键key
    key = ['user_id', 'username', 'urlname', 'created_at', 
            'pin_count', 'board_count', 'like_count', 
            'follower_count', 'following_count', 
            'boards_like_count', 'tag_count', 'crawl_at']
    # 值value
    value = ['INT NOT NULL', 'TEXT', 'TEXT', 'INT', 'INT', 
            'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT']

    # 调用MySQL新建函数
    mysql_create(db, key, value)

def mysql_create_pin(opt):
    # 用户数据库
    db = '`huaban2`.`pin.{}.{}`'.format(opt.cat, opt.db)
    # 健key
    key = ['pin_id', 'user_id', 'board_id', 'file_id', 
            'link', 'raw_text', 'via', 'original', 
            'created_at', 'like_count', 'comment_count', 
            'repin_count', 'tags', 'category', 'crawl_at']
    # 值value
    value = ['INT NOT NULL', 'INT', 'INT', 'INT', 'TEXT', 
                'TEXT', 'INT', 'INT', 'INT', 'INT', 
                'INT', 'INT', 'TEXT', 'TEXT', 'INT']

    # 调用MySQL新建函数
    mysql_create(db, key, value)

def mysql_create_file(opt):
    # 用户数据库
    db = '`huaban2`.`file.{}.{}`'.format(opt.cat, opt.db)
    # 健key
    key = ['id', 'key', 'type', 'width', 'height', 'crawl_at']
    # 值value
    value = ['INT NOT NULL', 'TEXT', 'TEXT', 'INT', 'INT', 'INT']

    # 调用MySQL新建函数
    mysql_create(db, key, value)

def scrapy_crawl(spider, opt):
    # 配置爬虫
    setting = get_project_settings()
    # 日志文件
    setting.set('LOG_FILE', 
        'huaban2.{}.{}.log'.format(opt.cat, opt.db))
    proc = CrawlerProcess(setting)
    proc.crawl(spider, ip=opt.ip, usr=opt.usr, 
            pwd=opt.pwd, db=opt.db, cat=opt.cat, 
            start=opt.start, req=opt.req, end=opt.end)
    # 开始爬取结构化数据
    proc.start()

if __name__ == '__main__':
    psr = OptionParser(version='%prog Version 1.0')

    # MySQL数据库IP地址，默认localhost
    psr.add_option('-i', '--ip', dest='ip', default='localhost', 
            help='IP Address for MySQL [Default: %default]')
    # MySQL数据库用户名，默认root用户
    psr.add_option('-u', '--usr', dest='usr', default='root', 
            help='Username for MySQL [Default: %default]')
    # MySQL数据库密码，默认123456
    psr.add_option('-p', '--pwd', dest='pwd', default='123456', 
            help='Password for MySQL [Default: %default]')
    # 新建MySQL Table，默认False
    psr.add_option('-n', '--new', dest='new', default=False, 
            help='Create MySQL Table', action='store_true')
    # MySQL Table后缀名，默认20170801
    psr.add_option('-d', '--db', dest='db', default='20170801', 
            help='MySQL Table DataBase [Default: %default]')
    # 花瓣网类别，默认web_app_icon
    psr.add_option('-c', '--cat', dest='cat', default='web_app_icon', 
            help='Huaban Category [Default: %default]')
    # 爬虫起始标识，默认画板id——40000000
    psr.add_option('-s', '--start', dest='start', type='int', 
            default=40000000, help='Scrapy Start [Default : %default]')
    # 起始请求类别，默认画板瀑布流
    psr.add_option('-r', '--req', dest='req', default='board', 
            help='Start URL Request [Default: %default]')
    # 爬虫结束标识，默认画板id——0
    psr.add_option('-e', '--end', dest='end', type='int', default=0, 
            help='Scrapy End URL [Default: %default]')

    # 解析参数
    opt, arg = psr.parse_args()

    if opt.new:
        # 新建画板数据库
        mysql_create_board(opt)
        # 新建用户数据库
        mysql_create_user(opt)
        # 新建采集数据库
        mysql_create_pin(opt)
        # 新建文件数据库
        mysql_create_file(opt)

    scrapy_crawl('fav_board', opt)
