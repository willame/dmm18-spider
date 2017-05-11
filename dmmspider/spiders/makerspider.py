# coding=utf-8
import scrapy
import MySQLdb
import re
import time
from random import randint

class makerSpider(scrapy.Spider):
    name = "makerspider"
    scheme = 'dmm18_all'
    countFailure = 0

    def start_requests(self):
        urls = self.retrieve_links("SELECT link from %s.dvd_links where cid not in (select cid from dvds) ORDER BY cid DESC"%self.scheme)# 255334/274552
        len(urls)
        # dvd_test = ['http://www.dmm.co.jp/mono/dvd/-/detail/=/cid=104absd01/',
        #             'http://www.dmm.co.jp/digital/videoa/-/detail/=/cid=sivr00003/',
        #             'http://www.dmm.co.jp/mono/dvd/-/detail/=/cid=tktek091/',
        #             'http://www.dmm.co.jp/mono/dvd/-/detail/=/cid=nnpj231/']
	#test = ['http://www.dmm.co.jp/mono/dvd/-/detail/=/cid=164sbci009/']
        for url in urls:
            request = scrapy.Request(url=url,callback=self.parse_video_info)
            request.meta['table'] = 'dvd_info'
            yield request
            # time.sleep(2)


    def parse_maker_page(self, response):
        # extract alphabet links
        alphabet_links = response.css('table.menu_aiueo td a::attr(href)').extract()
        # make new requests
        # self.log(len(alphabet_links))
        for url in alphabet_links:
            if not re.match(r'http:\/\/www\.dmm\.co\.jp', url):
                url = 'http://www.dmm.co.jp' + url
            # self.log("[PAL] " + url)
            request = scrapy.Request(url=url, callback=self.parse_maker_link)
            request.meta['table'] = response.meta['table']
            yield request

    def parse_maker_link(self, response):
        maker_links = response.css(u'table[summary="おすすめメーカー"] div.maker-text a::attr(href)').extract()
        maker_links += response.css(u'table[summary="メーカー一覧リスト"] a::attr(href)').extract()
        query_list = list()
        log_list = list()
        for maker_link in maker_links:
            if not re.match(r'http:\/\/www\.dmm\.co\.jp',maker_link):
                maker_link = 'http://www.dmm.co.jp'+maker_link
                if re.search(r'\/id=(.+)\/', maker_link):
                    id = re.search(r'\/id=(.+)\/', maker_link).group(1)
                    # self.log(id)
                    query = "INSERT INTO %s.%s(id,link) VALUES(\'%s\',\'%s\')"%(self.scheme, response.meta['table'],
                                                                                id, maker_link)
                    log = "[MAKER] %s"%id
                    query_list.append(query)
                    log_list.append(log)
                    # self.log(query)
        # self.log(len(maker_links))
        self.insert_data(query_list, log_list)

  

   

   
    def insert_data(self, query_list, log_list):
        # connect to database
        db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                                 user="dmm18",  # your username
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
