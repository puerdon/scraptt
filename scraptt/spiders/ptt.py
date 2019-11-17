# -*- coding: utf-8 -*-
"""Main crawler."""
import re
from datetime import datetime
from itertools import groupby

import scrapy
import dateutil.parser as dp

from .parsers.post import mod_content, extract_author, extract_ip
from .parsers.comment import comment_counter, split_ip_and_publish_time
from ..items import PostItem


class PttSpider(scrapy.Spider):
    """Crawler for PTT."""

    name = 'ptt'
    allowed_domains = ['ptt.cc']
    handle_httpstatus_list = [404]
    custom_settings = {
        'ITEM_PIPELINES': {
           # 'scraptt.pipelines.PTTPipeline': 300,
           # 'scraptt.pipelines.ElasticsearchPipeline': 400,
           'scraptt.pipelines.JsonPipeline': 500
        }
    }

    def __init__(self, *args, **kwargs):
        """__init__ method.

        :param: boards: comma-separated board list
        :param: since: start crawling from this date (format: YYYYMMDD)
        """
        # 從 scrapy 指令參數中擷取 boards 參數
        boards = kwargs.pop('boards')
        if boards == '_all':
            from cockroach.db import Session, Meta
            session = Session()
            self.boards = [i[0] for i in session.query(Meta.name)]
            session.close()
        else:   # 若要爬多個版，那麼版跟版之間以逗號分隔 E.g. -d boards=Gossiping,WomenTalk
            self.boards = boards.strip().split(',')

        if 'ALLPOST' in self.boards:
            self.boards.remove('ALLPOST')
            self.logger.warning('No support for crawling "ALLPOST"')

        since = kwargs.pop('since', None)
        # self.since = (
        #     datetime.strptime(since, '%Y%m%d').date()
        #     if since is not None
        #     else datetime.now().date()
        # )
        self.since = (
            datetime.strptime(since, '%Y%m%d').date()
            if since is not None
            else None
        )
        self.logger.warning(f"parameter 'since' detected!: {self.since}")

        year = kwargs.pop('year', None)
        self.year = datetime.strptime(year, '%Y').date() if year is not None else None
        self.index = 1
        self.logger.warning(f"接收year參數: {self.year}")

        # # Debug 用
        # self.output_path = kwargs.pop('output_path', None)
        # self.logger.warning(f"接收output_path參數: {self.output_path}")

        self.all_index = True if kwargs.pop('all_index', None) is not None else False


    def start_requests(self):
        """
        spider首個會呼叫的方法
        """
        for board in self.boards:
            if self.since or self.all_index:
                url = f'https://www.ptt.cc/bbs/{board}/index.html'
            elif self.year:
                url = f'https://www.ptt.cc/bbs/{board}/index{self.index}.html'
            else:
                self.logger.warning(f"沒有since參數也沒有year參數")
                return
            yield scrapy.Request(
                url,
                cookies={'over18': '1'},
                callback=self.parse_index
            )

    def parse_index(self, response):
        """
        Parse index pages.
        排除置底文
        """
        # 索引頁面中每篇po文的連結 CSS
        item_css = '.r-ent .title a'

        if response.url.endswith('index.html'):
            # 只有在 index.html 時會需要處理置底文的情況
            topics = response.dom('.r-list-sep').prev_all(item_css)
        else:
            topics = response.dom(item_css)

        list_of_topics = list(topics.items())

        if self.all_index:
            # 找出"上頁"按鈕的連結
            prev_url = response.dom('.btn.wide:contains("上頁")').attr('href')
            self.logger.info(f'index link: {prev_url}')
            latest_index = re.search(r"index(\d{1,6})\.html", prev_url).group(1)
            self.logger.info(f'latest_index: {latest_index}')
            latest_index = int(latest_index)
            self.logger.info(f'response.url: {response.url}')
            board = re.search(r"www\.ptt\.cc\/bbs\/([\w\d\-_]{1,30})\/", response.url).group(1)

            for index in range(1, latest_index + 1):
                url = f"https://www.ptt.cc/bbs/{board}/index{index}.html"
                self.logger.info(f"index link: {url}")

                yield scrapy.Request(
                    url,
                    cookies={'over18': '1'},
                    callback=self.parse_index_2
                )

        elif self.since is not None: # 只有在有 since 參數的情況下，才需要
            
            # reverse order to conform to timeline
            for topic in reversed(list_of_topics):
                title = topic.text()      # po文標題
                href = topic.attr('href') # po文連結
                timestamp = re.search(r'(\d{10})', href).group(1)
                post_time = datetime.fromtimestamp(int(timestamp)) # po文日期

                if post_time.date() < self.since:  # 如果po文時間比我們要的最早時間還要早
                    return
                self.logger.info(f'+ {title}, {href}, {post_time}')
                yield scrapy.Request(
                    href, cookies={'over18': '1'}, callback=self.parse_post
                )
            # 找出"上頁"按鈕的連結
            prev_url = response.dom('.btn.wide:contains("上頁")').attr('href')
            self.logger.info(f'index link: {prev_url}')
            if prev_url:

                yield scrapy.Request(
                    prev_url, cookies={'over18': '1'}, callback=self.parse_index
                )
        
        elif self.year is not None:
            for topic in list_of_topics[:-1]:
                title = topic.text()      # po文標題
                href = topic.attr('href') # po文連結
                timestamp = re.search(r'(\d{10})', href).group(1)
                post_time = datetime.fromtimestamp(int(timestamp)) # po文日期

                if post_time.year > self.year.year: # 如果po文年份比我們要的年份更晚
                    break

                elif post_time.year < self.year.year:
                    continue
                else:
                    self.logger.info(f'+ {title}, {href}, {post_time}')
                    yield scrapy.Request(
                        href,
                        cookies={'over18': '1'},
                        callback=self.parse_post
                    )

            last_post_in_a_index_page = list_of_topics[-1]
            href = last_post_in_a_index_page.attr('href') # po文連結
            timestamp = re.search(r'(\d{10})', href).group(1)
            post_time = datetime.fromtimestamp(int(timestamp)) # po文日期

            board = re.search(r"www\.ptt\.cc\/bbs\/([\w\d\-_]{1,30})\/", href).group(1)
            self.index += 1
            new_page_url = f"https://www.ptt.cc/bbs/{board}/index{self.index}.html"
            self.logger.info(f"new_page_url: {new_page_url}")
            
            if post_time.year > self.year.year: # 如果po文年份比我們要的年份更晚
                return

            elif post_time.year < self.year.year:
                self.logger.info('此頁最後一篇貼文年份小於目標年份，繼續爬下一個index')
                yield scrapy.Request(
                    new_page_url,
                    cookies={'over18': '1'},
                    callback=self.parse_index
                ) 
            else:
                self.logger.info('等魚')
                self.logger.info(f'+ {title}, {href}, {post_time}')
                
                yield scrapy.Request(
                    href,
                    cookies={'over18': '1'},
                    callback=self.parse_post
                ) 
                yield scrapy.Request(
                    new_page_url,
                    cookies={'over18': '1'},
                    callback=self.parse_index
                )

    def parse_index_2(self, response):
        item_css = '.r-ent .title a'

        if response.url.endswith('index.html'):
            # 只有在 index.html 時會需要處理置底文的情況
            topics = response.dom('.r-list-sep').prev_all(item_css)
        else:
            topics = response.dom(item_css)

        list_of_topics = list(topics.items())

        for topic in list_of_topics:
            title = topic.text()      # po文標題
            href = topic.attr('href') # po文連結
            timestamp = re.search(r'(\d{10})', href).group(1)
            post_time = datetime.fromtimestamp(int(timestamp)) # po文日期

            self.logger.info(f'+ {title}, {href}, {post_time}')
            yield scrapy.Request(
                href,
                cookies={'over18': '1'},
                callback=self.parse_post
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

        if response.status == 404:
            self.logger.warning(f'404: {response.url}')
            return None

        # 抓出主文
        content = (
            response.dom('#main-content')
            .clone()
            .children()
            .remove('span[class^="article-meta-"]')
            .remove('div.push')
            .end()
            .html()
        )

        # 抓出meta: 作者/看板/標題/時間
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

        # 目前為止的 post 字典有 ip, content, board, id
        post = {
            'ip': extract_ip(content),
            'content': mod_content(content),
            'board': response.dom('#topbar a.board').remove('*').text().strip(),
            'id': response.url.split('/')[-1].split('.html')[0]
        }
        # post = dict()

        # # 抽取文章ip
        # post['ip'] = extract_ip(content)

        # # 抽取清洗過的內文
        # post['content'] = mod_content(content)

        # # 抽取版名
        # post['board'] = (
        #     response.dom('#topbar a.board').remove('*').text().strip()
        # )

        # # 從URI抽取文章ID
        # post['id'] = (
        #     response.url
        #     .split('/')[-1]
        #     .split('.html')[0]
        # )


        # 將上面抽取的 meta 放進 post 字典，也就是多加 author / title / published
        # meta_mod = dict()
        for k in meta.keys():
            if k in ref:
                post[ref[k]] = meta[k].strip()

        # 確認是否有作者
        if 'author' in post:
            # 如果確認有作者，那麼抽離出作者的id，去掉暱稱
            post['author'] = extract_author(post['author'])
        else:
            # 如果確認沒有作者，那麼就不要這筆資料了
            self.logger.warning(f'no author found: {response.url}')
            return


        # 處理下方推文
        # 推文旁邊的 IP/日期/時間 不一定每條都是三個都有:
        # - 218.166.4.106 06/22
        # - 05/30 18:28
        comments = []
        for _ in response.dom('.push').items():
            published, ip = split_ip_and_publish_time(_('.push-ipdatetime').text())
            if published is None and ip is None:
            # 這種情況下，不是真的回文，而常常是樓主複製前面已出現過的回文
                continue

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
            
            # BEGIN: 找出回文發表時間
            # 如果前面的 published is None, 還不確定是否還需要下面這一段
            # 要再確認可不可以刪掉
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
            # END: 找出回文發表時間

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


        yield PostItem(**post)
