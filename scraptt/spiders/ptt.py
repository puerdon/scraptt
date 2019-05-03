# -*- coding: utf-8 -*-
"""Main crawler."""
import re
from datetime import datetime
from itertools import groupby

import scrapy
import dateutil.parser as dp

from .parsers.post import mod_content, extract_author, extract_ip
from .parsers.comment import comment_counter, remove_ip
from ..items import PostItem


class PttSpider(scrapy.Spider):
    """Crawler for PTT."""

    name = 'ptt'
    allowed_domains = ['ptt.cc']
    handle_httpstatus_list = [404]
    custom_settings = {
        'ITEM_PIPELINES': {
            'scraptt.pipelines.PTTPipeline': 300,
            'scraptt.pipelines.ElasticsearchPipeline': 400,
        }
    }

    def __init__(self, *args, **kwargs):
        """__init__ method.

        :param: boards: comma-separated board list
        :param: since: start crawling from this date (format: YYYYMMDD)
        """
        boards = kwargs.pop('boards')
        if boards == '_all':
            from cockroach.db import Session, Meta
            session = Session()
            self.boards = [i[0] for i in session.query(Meta.name)]
            session.close()
        else:
            self.boards = boards.strip().split(',')
        if 'ALLPOST' in self.boards:
            self.boards.remove('ALLPOST')
            self.logger.warning('No support for crawling "ALLPOST"')
        since = kwargs.pop('since', None)
        self.since = (
            datetime.strptime(since, '%Y%m%d').date()
            if since
            else datetime.now().date()
        )
        self.logger.warning(f"parameter 'since' detected!: {self.since}")

    def start_requests(self):
        """Request handler."""
        for board in self.boards:
            url = f'https://www.ptt.cc/bbs/{board}/index.html'
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
        for topic in reversed(list(topics.items())):
            title = topic.text()
            href = topic.attr('href')
            timestamp = re.search(r'(\d{10})', href).group(1)
            time = datetime.fromtimestamp(int(timestamp))
            if time.date() < self.since:
                return
            # self.logger.info(f'+ {title}, {href}, {time}')
            yield scrapy.Request(
                href, cookies={'over18': '1'}, callback=self.parse_post
            )
        prev_url = response.dom('.btn.wide:contains("上頁")').attr('href')
        if prev_url:
            yield scrapy.Request(
                prev_url, cookies={'over18': '1'}, callback=self.parse_index
            )

    def parse_post(self, response):
        """
        解析PTT上的每一篇Post。
        目標是產生一個名為post的dict，具有以下鍵值：
        {
            ip: 
            author: 作者id
            time: 文章標題
            content: 文章內文
            board: 版名
            id: 文章id
            quote: <String>                 # 如果是回文的話，此欄位存引述的內容
            time: {
                published: <Datetime>,
                crawled: <Datatime>
            }    
            comments: [
                {
                    type: "推|噓|→",
                    author: ,
                    content: ,
                    time: {
                        published: <Datetime>,
                        crawled: <Datetime>
                    },
                    ip: 
                },
                
            count: {                       #推,噓,回文數量 <defaultdict>
                "推": <int>,
                "噓": <int>,
                "→": <int>

            } 

        """
        # print(response.body)

        if response.status == 404:
            self.logger.warning(f'404: {response.url}')
            return None
        content = (
            response.dom('#main-content')
            .clone()
            .children()
            .remove('span[class^="article-meta-"]')
            .remove('div.push')
            .end()
            .html()
        )
        meta = dict(
            (_.text(), _.next().text())
            for _
            in response.dom('.article-meta-tag').items()
        )
        ref = {
            '作者': 'author',
            '時間': 'published',
            '標題': 'title',
        }
        post = dict()
        post['ip'] = extract_ip(content)
        post['content'] = mod_content(content)
        post['board'] = (
            response.dom('#topbar a.board').remove('*').text().strip()
        )
        post['id'] = (
            response.url
            .split('/')[-1]
            .split('.html')[0]
        )
        meta_mod = dict()
        for k in meta.keys():
            if k in ref:
                meta_mod[ref[k]] = meta[k].strip()
        comments = []
        for _ in response.dom('.push').items():
            published, ip = remove_ip(_('.push-ipdatetime').text())
            comment = {
                'type': _('.push-tag').text(),
                'author': extract_author(_('.push-userid').text()),
                'content': _('.push-content').text().lstrip(' :'),
                'time': {
                    'published': published,
                    'crawled': datetime.now().replace(microsecond=0),
                },
                'ip': ip,
            }
            time_cands = re.findall(
                '\d{1,2}/\d{1,2}\s\d{1,2}:\d{1,2}',
                comment['time']['published']
            )
            if time_cands:
                comment['time']['published'] = time_cands[-1]
                comments.append(comment)
            else:
                self.logger.warning(
                    (
                        'Unknown comment published time detected!\n'
                        f'url: {response.url}\n'
                        f'author: {comment["author"]}'
                    )
                )

        post.update(meta_mod)
        if 'author' in post:
            post['author'] = extract_author(post['author'])
        else:
            self.logger.warning(f'no author found: {response.url}')
            return

        post['time'] = {
            'published': dp.parse(post.pop('published'))
        }
        post['comments'] = comments

        # Merge comments with consecutive comments with the same author.
        con = []
        for author, group in groupby(comments, key=lambda x: x['author']):
            d = {}
            for comment in group:
                if d:
                    d['content'] += comment['content']
                else:
                    d = comment
            con.append(d)

        # add YEAR to comments
        year = post['time']['published'].year
        latest_month = post['time']['published'].month
        current_year = datetime.now().year
        _comments = []
        for comment in comments:
            try:
                published = dp.parse(comment['time']['published'])
                _comments.append(comment)
            except ValueError:
                self.logger.error(
                    (
                        f"unknown format: {comment['time']['published']} "
                        f"(author: {comment['author']} | {response.url} )"
                    )
                )
                continue
            if (
                published.month < latest_month and
                published.year < current_year
            ):
                year += 1
            comment['time']['published'] = published.replace(year=year)
            latest_month = published.month
        post['comments'] = _comments
        # quote
        msg = post['content']
        qs = re.findall('※ 引述.*|\n: .*', msg)
        for q in qs:
            msg = msg.replace(q, '')
        qs = '\n'.join([i.strip('\n') for i in qs])
        post['content'] = msg.strip('\n ')
        if qs:
            post['quote'] = qs
        post['time']['crawled'] = datetime.now().replace(microsecond=0)

        # 推噓文數量
        post.update(
            {'count': comment_counter(post['comments'])}
        )
        # print(post)
        yield PostItem(**post)
