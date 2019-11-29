# -*- coding: utf-8 -*-
"""Meta crawler."""

import scrapy
import re
import copy
from ..items import MetaItem


class MetaSpider(scrapy.Spider):
    """Get all PTT boards."""

    name = 'meta'
    allowed_domains = ['ptt.cc']
    # start_urls = ['https://www.ptt.cc/cls/3297']
    custom_settings = {
        'ITEM_PIPELINES': {
            'scraptt.pipelines.MetaExportPipeline': 300
        }
    }

    def __init__(self, *args, **kwargs):
        """__init__ method.

        :param: boards: comma-separated board list
        :param: since: start crawling from this date (format: YYYYMMDD)
        """
        self.index = kwargs.get('index', '1')
        self.logger.info(f"start class index: {self.index}")

    def start_requests(self):
        yield scrapy.Request(
            f"https://www.ptt.cc/cls/{self.index}",
            callback=self.parse
        )


    def parse(self, response, parent_nodes=None):
        """Parse DOM."""
        self.logger.info("==========")
        self.logger.info("呼叫parse()")
        self.logger.info("parent_nodes 參數:")
        self.logger.info(parent_nodes)
        self.logger.info(f"即將要loop {[xx.children('.board-name').text() for xx in response.dom('.b-ent a').items()]}")
        for i, _ in enumerate(response.dom('.b-ent a').items()):


            self.logger.info(f"loop - {i}")

            href = _.attr('href')
            board_name = _.children('.board-name').text()
            board_class = _.children('.board-class').text()
            board_title = _.children('.board-title').text()

            flag = '/index.html'
            if href.endswith(flag):
                self.logger.info("== 本頁是 .html")
                # board_name = href.replace(flag, '').split('/')[-1]
                if board_name == 'ALLPOST' or board_name == '0ClassRoot' or board_name == 'PttAllPosts':
                    # "ALLPOST" always return 404, so it's pointless to
                    # crawl this board.
                    return
                self.logger.info("因此準備輸出成 ITEM")
                self.logger.info("@@@@ ITEM @@@@")
                self.logger.info(MetaItem(board_name=board_name, board_class=board_class, board_title=board_title, parent_nodes=parent_nodes))  
                yield MetaItem(board_name=board_name, board_class=board_class, board_title=board_title, parent_nodes=parent_nodes)
            else:
                self.logger.info("== 本頁不是 .html")
                self.logger.info("== 生成 parent_obj")

                board_class_id = re.findall(r"(\d{1,10})", href)[0]
                parent_obj = {
                    "board_name": board_name,
                    "board_class": board_class,
                    "board_title": board_title,
                    "board_class_id": board_class_id
                }

                if parent_nodes is not None and isinstance(parent_nodes, list):
                    self.logger.info("==== parent_nodes is not None")
                    
                    ppp = copy.deepcopy(parent_nodes)


                    ppp.append(parent_obj)

                    self.logger.info("==== 將parent_obj更新進parent_nodes")
                    yield scrapy.Request(href, self.parse, cb_kwargs=dict(parent_nodes=ppp))

                else:
                    self.logger.info("==== parent_nodes is None")

                    p = list()
                    p.append(parent_obj)
                    self.logger.info("==== 將p更新進parent_nodes")

                    yield scrapy.Request(href, self.parse, cb_kwargs=dict(parent_nodes=p))
                    # parent_nodes = None
                    # self.logger.info("把 parent_nodes 清為 None")
