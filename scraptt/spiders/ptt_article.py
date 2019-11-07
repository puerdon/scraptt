# -*- coding: utf-8 -*-
"""Main crawler."""
import re
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
        }
    }

    def __init__(self, *args, **kwargs):
        """__init__ method.

        :param: boards: comma-separated board list
        :param: since: start crawling from this date (format: YYYYMMDD)
        """
        self.board = kwargs.pop('board')
        self.pages = kwargs.pop('pages')


    def start_requests(self):
        """Request handler."""
        for i in range(1, int(self.pages) + 1):
            url = f'https://www.ptt.cc/bbs/{self.board}/index{i}.html'
            yield scrapy.Request(
                url, cookies={'over18': '1'}, callback=self.parse_index
            )

    def parse_index(self, response):
        """Parse index pages."""
        # exclude "置底文"
        # print(response.body)
        item_css = '.r-ent .title a'
        if response.url.endswith('index.html'):
            topics = response.dom('.r-list-sep').prev_all(item_css)
        else:
            topics = response.dom(item_css)

        # reverse order to conform to timeline
        for topic in list(topics.items()):
            title = topic.text()
            href = topic.attr('href')
            yield scrapy.Request(
                href, cookies={'over18': '1'}, callback=self.parse_post
            )


    def parse_post(self, response):
        """


        """
        con = response.body.decode(response.encoding)
        time = None
        article_id = response.url.split('/')[-1].split('.html')[0]
        for t in response.dom('.article-meta-tag').items():
            if t.text().strip() == "時間":
                time = dp.parse(t.next().text())
        try:
            with open(f"data/{time.year}-{time.month}-{time.day}_{article_id}.html", "w") as f:
                f.write(con)
        except Exception as e:
            self.logger.warning(e)
            self.logger.warning(f"有問題的文章iD: {article_id}")

        yield _ArticleItem(article=con)
    #     # print(response.body)

    #     if response.status == 404:
    #         self.logger.warning(f'404: {response.url}')
    #         return None

    #     # 抓出主文
    #     content = (
    #         response.dom('#main-content')
    #         .clone()
    #         .children()
    #         .remove('span[class^="article-meta-"]')
    #         .remove('div.push')
    #         .end()
    #         .html()
    #     )

    #     # 抓出meta: 作者/看板/標題/時間
    #     meta = dict(
    #         (_.text(), _.next().text())
    #         for _
    #         in response.dom('.article-meta-tag').items()
    #     )

    #     ref = {
    #         '作者': 'author',
    #         '時間': 'published',
    #         '標題': 'title',
    #     }

    #     # 先形成初步的 post 字典
    #     post = {
    #         'ip': extract_ip(content),
    #         'content': mod_content(content),
    #         'board': response.dom('#topbar a.board').remove('*').text().strip(),
    #         'id': response.url.split('/')[-1].split('.html')[0]
    #     }
    #     # post = dict()

    #     # # 抽取文章ip
    #     # post['ip'] = extract_ip(content)

    #     # # 抽取清洗過的內文
    #     # post['content'] = mod_content(content)

    #     # # 抽取版名
    #     # post['board'] = (
    #     #     response.dom('#topbar a.board').remove('*').text().strip()
    #     # )

    #     # # 從URI抽取文章ID
    #     # post['id'] = (
    #     #     response.url
    #     #     .split('/')[-1]
    #     #     .split('.html')[0]
    #     # )


    #     # 將上面抽取的 meta 放進 post 字典，也就是多加 author / title / published
    #     # meta_mod = dict()
    #     for k in meta.keys():
    #         if k in ref:
    #             post[ref[k]] = meta[k].strip()

    #     # 確認是否有作者
    #     if 'author' in post:
    #         post['author'] = extract_author(post['author'])
    #     else:
    #         self.logger.warning(f'no author found: {response.url}')
    #         return


    #     # 處理下方推文
    #     comments = []
    #     for _ in response.dom('.push').items():
    #         published, ip = split_ip_and_publish_time(_('.push-ipdatetime').text())

    #         if published is None and ip is None:
    #         # 這種情況下，不是真的回文，而常常是樓主複製前面已出現過的回文
    #             continue

    #         comment = {
    #             'type': _('.push-tag').text(),
    #             'author': extract_author(_('.push-userid').text()),
    #             'content': _('.push-content').text().lstrip(' :'),
    #             'time': {
    #                 'published': published,
    #                 'crawled': datetime.now().replace(microsecond=0),
    #             },
    #             'ip': ip,
    #         }
            
    #         # BEGIN: 找出回文發表時間
    #         # 如果前面的 published is None, 還不確定是否還需要下面這一段
    #         # 要再確認可不可以刪掉
    #         time_cands = re.findall(
    #             '\d{1,2}/\d{1,2}\s\d{1,2}:\d{1,2}',
    #             comment['time']['published']
    #         )
    #         if time_cands:
    #             comment['time']['published'] = time_cands[-1]
    #             comments.append(comment)
    #         else:
    #             self.logger.warning(
    #                 (
    #                     'Unknown comment published time detected!\n'
    #                     f'url: {response.url}\n'
    #                     f'author: {comment["author"]}'
    #                 )
    #             )
    #         # END: 找出回文發表時間

        
    #     # 把 meta_mod 字典融進 post 字典
    #     # post.update(meta_mod)
        


    #     post['time'] = {
    #         'published': dp.parse(post.pop('published'))
    #     }
    #     post['comments'] = comments

    #     # Merge comments with consecutive comments with the same author.
    #     con = []
    #     for author, group in groupby(comments, key=lambda x: x['author']):
    #         d = {}
    #         for comment in group:
    #             if d:
    #                 d['content'] += comment['content']
    #             else:
    #                 d = comment
    #         con.append(d)

    #     # add YEAR to comments
    #     year = post['time']['published'].year
    #     latest_month = post['time']['published'].month
    #     current_year = datetime.now().year
    #     _comments = []
    #     for comment in comments:
    #         try:
    #             published = dp.parse(comment['time']['published'])
    #             _comments.append(comment)
    #         except ValueError:
    #             self.logger.error(
    #                 (
    #                     f"unknown format: {comment['time']['published']} "
    #                     f"(author: {comment['author']} | {response.url} )"
    #                 )
    #             )
    #             continue
    #         if (
    #             published.month < latest_month and
    #             published.year < current_year
    #         ):
    #             year += 1
    #         comment['time']['published'] = published.replace(year=year)
    #         latest_month = published.month
    #     post['comments'] = _comments
    #     # quote
    #     msg = post['content']
    #     qs = re.findall('※ 引述.*|\n: .*', msg)
    #     for q in qs:
    #         msg = msg.replace(q, '')
    #     qs = '\n'.join([i.strip('\n') for i in qs])
    #     post['content'] = msg.strip('\n ')
    #     if qs:
    #         post['quote'] = qs
    #     post['time']['crawled'] = datetime.now().replace(microsecond=0)

    #     # 推噓文數量
    #     post.update(
    #         {'count': comment_counter(post['comments'])}
    #     )
    #     # print(post)
    #     yield PostItem(**post)
