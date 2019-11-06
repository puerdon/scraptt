from hashlib import sha256
import logging
import pymongo
import urllib
import os

from jseg import Jieba
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import (
    Document,
    Text,
    Integer,
    Date,
)

j = Jieba()

ELASTICSEARCH_HOST = ""
ELASTICSEARCH_PORT = ""

logger = logging.getLogger(__name__)

# define a default connection to ES that can be used globally
connections.create_connection(
    hosts=['{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}'.format(**os.environ)],
)


def seg_str(text):
    """
    將尚未分詞的完整句子使用jieba分詞，回傳以空格隔開的字串。
    Str -> Str
    Input: 未分詞的字串 <str>
    Output: 經過結巴分詞後的字串，詞與詞之間以空格隔開 <str>
    """
    if not text:
        return ''
    output = ' '.join(j.seg(text))
    return output


class Doc(Document):
    """
    定義Elasticsearch Documentation的mapping
    參考：https://elasticsearch-dsl.readthedocs.io/en/latest/persistence.html
    """

    post_type = Integer(required=True)
    board = Text(required=True)
    author = Text(required=True)
    published = Date(required=True)
    title = Text()
    content = Text(required=True)
    ip = Text()
    upvote = Integer()
    novote = Integer()
    downvote = Integer()
    type = Text()
    post_id = Text()

    class Index:
        """Index info."""

        name = 'ptt'


class Mongo2ESDoc(Doc):
    """ES Doc for CockroachDB."""

    def __init__(self, *args, **kwargs):
        """Post process for fields."""
        if 'title' in kwargs:
            kwargs['title'] = seg_str(kwargs['title'])
        kwargs['content'] = seg_str(kwargs['content'])
        super().__init__(*args, **kwargs)
        self.meta.id = kwargs.pop('id')



class MongoPipeline(object):

    collection_name = 'scrapy_items'

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri
        self.username = urllib.parse.quote_plus('lope')
        self.password = urllib.parse.quote_plus('ntugillope')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
        )

    def open_spider(self, spider):

        self.client = pymongo.MongoClient('mongodb://{}:{}@{}'.format(self.username, self.password, self.mongo_uri))

        dblist = self.client.list_database_names()
        if 'ptt' not in dblist:
            logging.warn("There is not db named 'ptt'. Creating one...")

        self.pttdb = self.client["ptt"]


        # col_list = self.pttdb.list_collection_names()

        # if "meta" not in col_list:
        self.meta_col = self.pttdb["meta"]
        # else:
            # self.meta_col = self.pttdb["meta"]

        # if "ptt" not in col_list:
            # self.ptt_col = self.pttdb["ptt"]
        # else:
        self.ptt_col = self.pttdb["ptt"]

    def close_spider(self, spider):
        self.client.close()


class MetaPipeline(MongoPipeline):
    """Insert PTT meta-data into database."""

    def process_item(self, item, spider):
        """Insert data into database."""

        meta_obj = {
            "name": item["name"]
        }

        self.meta_col.insert_one(meta_obj)

        return item

class PTTPipeline(MongoPipeline):
    """Insert PTT POST and COMMENT into database."""

    # 其實還需要檢查蟲爬的文章是否已經重複或者完全沒有更動，所以不用浪費資源再存一次
    # 或者要使用mongodb的更新功能

    def process_item(self, item, spider):
        """Insert data into database."""
        post_obj = {
            "id": item['id'],
            "board": item['board'],
            "author": item['author'],
            "published": item['time']['published'],
            "crawled": item['time']['crawled'],
            "title": item['title'],
            "ip": item['ip'],
            "content": item['content'],
            "upvote": item['count']['推'],
            "novote": item['count']['→'],
            "downvote": item['count']['噓'],
        }

        # self.ptt_col.insert_one(post_obj)
        self.ptt_col.update_one(
            {"id": post_obj["id"]},
            {"$set": post_obj},
            upsert=True
        )


        if len(item['comments']) == 0:
            return item
        else:
            for comment in item['comments']:
                hashid = sha256((f"{item['id']}"f"{comment['author']}"f"{comment['time']['published']}").encode('utf-8')).hexdigest()[:16]
                comment_obj = {
                    "id": hashid,
                    "type": comment['type'],
                    "author": comment['author'],
                    "published": comment['time']['published'],
                    "crawled": comment['time']['crawled'],
                    "ip": comment['ip'],
                    "content": comment['content'],
                    "post_id": item['id'],
                }
                # self.ptt_col.insert_one(comment_obj)
                self.ptt_col.update_one(
                    {"id": comment_obj["id"]},
                    {"$set": comment_obj},
                    upsert=True
                )

        return item


class ElasticsearchPipeline:
    """Insert PTT POST and COMMENT into Elasticsearch."""

    def process_item(self, item, spider):
        """Insert data into database."""
        Mongo2ESDoc(
            post_type=0,
            board=item['board'],
            author=item['author'],
            published=item['time']['published'],
            title=item['title'],
            content=item['content'],
            ip=item['ip'],
            upvote=item['count']['推'],
            novote=item['count']['→'],
            downvote=item['count']['噓'],
            id=item['id'],
        ).save()
        for comment in item['comments']:
            hashid = sha256((
                f"{item['id']}"
                f"{comment['author']}"
                f"{comment['time']['published']}"
            ).encode('utf-8')).hexdigest()[:16]
            
            Mongo2ESDoc(
                id=hashid,
                type=comment['type'],
                post_type=1,
                board=item['board'],
                author=comment['author'],
                published=comment['time']['published'],
                ip=comment['ip'],
                content=comment['content'],
                post_id=item['id'],
            ).save()
        return item


class JsonPipeline:
    def open_spider(self, spider):
        self.board_to_exporter = {}


    def close_spider(self, spider):
        for exporter in self.board_to_exporter.values():
            exporter.finish_exporting()
            exporter.file.close()

    def _exporter_for_item(self, item):
        board = item['board']
        if board not in self.board_to_exporter:
            f = open('/data/rawdata/{}.jsonl'.format(board), 'wb')
            exporter = JsonLinesItemExporter(f)
            exporter.start_exporting()
            self.board_to_exporter[board] = exporter
        return self.board_to_exporter[board]

    def process_item(self, item, spider):
        """Insert data into database."""
        post_obj = {
            "id": item['id'],
            "board": item['board'],
            "author": item['author'],
            "published": item['time']['published'],
            "crawled": item['time']['crawled'],
            "title": item['title'],
            "ip": item['ip'],
            "content": item['content'],
            "upvote": item['count']['推'],
            "novote": item['count']['→'],
            "downvote": item['count']['噓'],
        }

        # self.ptt_col.insert_one(post_obj)
        # self.ptt_col.update_one(
        #     {"id": post_obj["id"]},
        #     {"$set": post_obj},
        #     upsert=True
        # )


        if len(item['comments']) == 0:
            exporter = self._exporter_for_item(item)
            exporter.export_item(post_obj)

            return item
        else:
            for comment in item['comments']:
                hashid = sha256((f"{item['id']}"f"{comment['author']}"f"{comment['time']['published']}").encode('utf-8')).hexdigest()[:16]
                comment_obj = {
                    "id": hashid,
                    "type": comment['type'],
                    "author": comment['author'],
                    "published": comment['time']['published'],
                    "crawled": comment['time']['crawled'],
                    "ip": comment['ip'],
                    "content": comment['content'],
                    "post_id": item['id'],
                }
                # self.ptt_col.insert_one(comment_obj)
                # self.ptt_col.update_one(
                #     {"id": comment_obj["id"]},
                #     {"$set": comment_obj},
                #     upsert=True
                # )
                exporter = self._exporter_for_item(item)
                exporter.export_item(comment_obj)
        return item


if __name__ == "__main__":



