# -*- coding: utf-8 -*-
"""Main crawler."""
import re
import os
from datetime import datetime
from itertools import groupby

import scrapy
import dateutil.parser as dp

from .parsers.post import mod_content, extract_author, extract_ip
from .parsers.comment import comment_counter, split_ip_and_publish_time
from ..items import _ArticleItem


class PttSpider(scrapy.Spider):
    """Crawler for PTT."""

    name = 'ptt_article'
    allowed_domains = ['ptt.cc']
    handle_httpstatus_list = [404]
    custom_settings = {
        'ITEM_PIPELINES': {
           # 'scraptt.pipelines.PTTPipeline': 300,
           # 'scraptt.pipelines.ElasticsearchPipeline': 400,
           # 'scraptt.pipelines.JsonPipeline': 500
           'scraptt.pipelines.HTMLFilePipeline': 500

        }
    }

    def __init__(self, *args, **kwargs):
        """__init__ method.

        :param: boards: comma-separated board list
        :param: since: start crawling from this date (format: YYYYMMDD)
        """
        self.boards = kwargs.pop('boards').split(',')
        self.all = kwargs.pop('all', None)
        self.index_from = kwargs.pop('index_from', None)
        self.index_to = kwargs.pop('index_to', None)

        self.logger.info(f"boards: {self.boards}")
        self.logger.info(f"all: {self.all}")
        self.logger.info(f"pages: index{self.index_from} - index{self.index_to}")



    def start_requests(self):
        """Request handler."""
        if self.all is not None:
            for board in self.boards:
                url = f'https://www.ptt.cc/bbs/{board}/index.html'
                yield scrapy.Request(
                    url,
                    cookies={'over18': '1'},
                    callback=self.parse_latest_index
                )
        else:
            board = self.boards[0]
            for i in range(int(self.index_from), int(self.index_to) + 1):
                url = f'https://www.ptt.cc/bbs/{board}/index{i}.html'
                yield scrapy.Request(
                    url,
                    cookies={'over18': '1'},
                    callback=self.parse_index
                )

    def parse_index(self, response):
        """Parse index pages."""
        # exclude "置底文"
        item_css = '.r-ent .title a'
        if response.url.endswith('index.html'):
            topics = response.dom('.r-list-sep').prev_all(item_css)
        else:
            topics = response.dom(item_css)

        for topic in list(topics.items()):
            title = topic.text()
            href = topic.attr('href')
            yield scrapy.Request(
                href,
                cookies={'over18': '1'},
                callback=self.parse_post
            )

    def parse_latest_index(self, response):
        """Parse index pages."""
        # 找出"上頁"按鈕的連結
        prev_url = response.dom('.btn.wide:contains("上頁")').attr('href')
        self.logger.info(f'index link: {prev_url}')
        latest_index = re.search(r"index(\d{1,6})\.html", prev_url).group(1)
        self.logger.info(f'latest_index: {latest_index}')
        latest_index = int(latest_index)
        self.logger.info(f'response.url: {response.url}')
        board = re.search(r"www\.ptt\.cc\/bbs\/([\w\d\-_]{1,30})\/", response.url).group(1)
        print(f"board: {board}")
        print(f"latest index: {latest_index}")

        for index in range(1, latest_index + 1):
            url = f"https://www.ptt.cc/bbs/{board}/index{index}.html"
            self.logger.info(f"index link: {url}")

            yield scrapy.Request(
                url,
                cookies={'over18': '1'},
                callback=self.parse_index
            )


    def parse_post(self, response):
        """
        """
        board = re.search(r"www\.ptt\.cc\/bbs\/([\w\d\-_]{1,30})\/", response.url).group(1)
        con = response.body.decode(response.encoding)
        timestamp = re.search(r'(\d{10})', response.url).group(1)
        dt = datetime.fromtimestamp(int(timestamp))
        dt_str = dt.strftime("%Y%m%d_%H%M")
        article_id = response.url.split('/')[-1].split('.html')[0]

        article = {
            "board": board,
            "html_content": con,
            "timestamp": timestamp,
            "article_id": article_id
        }

        yield _ArticleItem(**article)
