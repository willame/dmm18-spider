# coding=utf-8
import scrapy
import MySQLdb
import re
import time
from random import randint

class makerSpider(scrapy.Spider):
    name = "linkspider"
    scheme = 'dmm18_all'
    

    def start_requests(self):
	
       #抓取dvd已发售前10页
	#抓取动画发售日排序前10页
	urls = [('http://www.dmm.co.jp/digital/videoa/-/list/=/limit=120/sort=release_date/page=%d/'%i,'video_links2') for i in range(1,11,1)]
	urls2 = [('http://www.dmm.co.jp/mono/dvd/-/list/=/limit=120/list_type=release/sort=date/page=%d/'%i,'dvd_links') for i in range(1,11,1) ]
	urls += urls2
        for url in urls:
            request = scrapy.Request(url=url[0],callback=self.parse_video_links)
            request.meta['table'] = url[1]
            yield request
            # time.sleep(2)

	#解析影片页面，每页120个影片，提取影片链接，存入links表
    def parse_video_links(self, response):
        video_links = response.css('p.tmb a::attr(href)').extract()
        query_list = list()
        log_list = list()
        for video_link in video_links:
            if re.search(r'\/cid=(.+)\/', video_link):
                cid = re.search(r'\/cid=(.+)\/', video_link).group(1)
                query = "INSERT INTO %s.%s(cid,link) VALUES(\'%s\',\'%s\')"\
                        %(self.scheme,response.meta['table'],cid,video_link)
                log = '[LINK] %s'%cid
                query_list.append(query)
                log_list.append(log)
                # self.log(log)

        # self.log(len(query_list))
        self.insert_data(query_list, log_list)

   
	#存入数据库
    def insert_data(self, query_list, log_list):
        # connect to database
        db = MySQLdb.connect(host="localhost",  # 记得修改为远端数据库ip
                                 user="root",  # your username
                                 passwd="666666",  # your password
                                 db="dmm18_all",  # name of the data base
                                 charset='utf8')  #

        cur = db.cursor()

        for i in range(0, len(query_list), 1):
            try:
                 # self.log(query_list[i])
                cur.execute(query_list[i])
                db.commit()
                self.log('[INSERT SUCCESS] %s' % log_list[i])
            except (MySQLdb.Error, MySQLdb.Warning) as e:
                db.rollback()
                self.log('[INSERT FAIL] %s' % log_list[i])
                self.log(e)

        db.close()


	#从数据库里拿数据
    def retrieve_links(self, query):
        # connect to database
        db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                             user="dmm18",  # your username
                             passwd="666666",  # your password
                             db="dmm18_all",  # name of the data base
                             charset='utf8')  #

        cur = db.cursor()
        cur.execute(query)
        links = [row[0] for row in cur.fetchall()]
        self.log('[RETRIEVE LINKS] %d' % len(links))
        db.close()
        return links

        # find the src url of large picture

   
   
   
