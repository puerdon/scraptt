"""Scrapy pipeilnes."""
from hashlib import sha256
import logging
import os

import pymongo
import urllib

from datetime import datetime
# from .es import Mongo2ESDoc
logger = logging.getLogger(__name__)

from scrapy.exporters import JsonLinesItemExporter

class CustomJsonLinesItemExporter(JsonLinesItemExporter):
    def __init__(self, file, **kwargs):
        # 將超類的ensure_ascii屬性設置為False， 確保輸出中文而不是其unicode編碼
        super(CustomJsonLinesItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)

# class MongoPipeline(object):

#     collection_name = 'scrapy_items'

#     def __init__(self, mongo_uri):
#         self.mongo_uri = mongo_uri
#         self.username = urllib.parse.quote_plus('lope')
#         self.password = urllib.parse.quote_plus('ntugillope')

#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             mongo_uri=crawler.settings.get('MONGO_URI'),
#         )

#     def open_spider(self, spider):

#         self.client = pymongo.MongoClient('mongodb://{}:{}@{}'.format(self.username, self.password, self.mongo_uri))

#         dblist = self.client.list_database_names()
#         if 'ptt' not in dblist:
#             logging.warn("There is not db named 'ptt'. Creating one...")

#         self.pttdb = self.client["ptt"]


#         # col_list = self.pttdb.list_collection_names()

#         # if "meta" not in col_list:
#         self.meta_col = self.pttdb["meta"]
#         # else:
#             # self.meta_col = self.pttdb["meta"]

#         # if "ptt" not in col_list:
#             # self.ptt_col = self.pttdb["ptt"]
#         # else:
#         self.ptt_col = self.pttdb["ptt"]

#     def close_spider(self, spider):
#         self.client.close()


# class MetaPipeline(MongoPipeline):
#     """Insert PTT meta-data into database."""

#     def process_item(self, item, spider):
#         """Insert data into database."""

#         meta_obj = {
#             "name": item["name"]
#         }

#         self.meta_col.insert_one(meta_obj)

#         return item

# class PTTPipeline(MongoPipeline):
#     """Insert PTT POST and COMMENT into database."""

#     # 其實還需要檢查蟲爬的文章是否已經重複或者完全沒有更動，所以不用浪費資源再存一次
#     # 或者要使用mongodb的更新功能

#     def process_item(self, item, spider):
#         """Insert data into database."""
#         post_obj = {
#             "id": item['id'],
#             "board": item['board'],
#             "author": item['author'],
#             "published": item['time']['published'],
#             "crawled": item['time']['crawled'],
#             "title": item['title'],
#             "ip": item['ip'],
#             "content": item['content'],
#             "upvote": item['count']['推'],
#             "novote": item['count']['→'],
#             "downvote": item['count']['噓'],
#         }

#         # self.ptt_col.insert_one(post_obj)
#         self.ptt_col.update_one(
#             {"id": post_obj["id"]},
#             {"$set": post_obj},
#             upsert=True
#         )


#         if len(item['comments']) == 0:
#             return item
#         else:
#             for comment in item['comments']:
#                 hashid = sha256((f"{item['id']}"f"{comment['author']}"f"{comment['time']['published']}").encode('utf-8')).hexdigest()[:16]
#                 comment_obj = {
#                     "id": hashid,
#                     "type": comment['type'],
#                     "author": comment['author'],
#                     "published": comment['time']['published'],
#                     "crawled": comment['time']['crawled'],
#                     "ip": comment['ip'],
#                     "content": comment['content'],
#                     "post_id": item['id'],
#                 }
#                 # self.ptt_col.insert_one(comment_obj)
#                 self.ptt_col.update_one(
#                     {"id": comment_obj["id"]},
#                     {"$set": comment_obj},
#                     upsert=True
#                 )

#         return item


# class ElasticsearchPipeline:
#     """Insert PTT POST and COMMENT into Elasticsearch."""

#     def process_item(self, item, spider):
#         """Insert data into database."""
#         Mongo2ESDoc(
#             post_type=0,
#             board=item['board'],
#             author=item['author'],
#             published=item['time']['published'],
#             title=item['title'],
#             content=item['content'],
#             ip=item['ip'],
#             upvote=item['count']['推'],
#             novote=item['count']['→'],
#             downvote=item['count']['噓'],
#             id=item['id'],
#         ).save()
#         for comment in item['comments']:
#             hashid = sha256((
#                 f"{item['id']}"
#                 f"{comment['author']}"
#                 f"{comment['time']['published']}"
#             ).encode('utf-8')).hexdigest()[:16]
            
#             Mongo2ESDoc(
#                 id=hashid,
#                 type=comment['type'],
#                 post_type=1,
#                 board=item['board'],
#                 author=comment['author'],
#                 published=comment['time']['published'],
#                 ip=comment['ip'],
#                 content=comment['content'],
#                 post_id=item['id'],
#             ).save()
#         return item


class HTMLFilePipeline:
    def process_item(self, item, spider):
        board = item['board']
        article_id = item['article_id']
        timestamp = item['timestamp']
        dt = datetime.fromtimestamp(int(timestamp))
        dt_str = dt.strftime("%Y%m%d_%H%M")
        
        try:
            logger.info(f"{board}-{dt}-{article_id}")
            # Server
            os.makedirs(f"/data/rawdata/{board}/{dt.year}", exist_ok=True)
            with open(f"/data/rawdata/{board}/{dt.year}/{dt_str}_{article_id}.html", "w+") as f:
                f.write(item['html_content'])
            # Debug
            # os.makedirs(f"data/{board}/{dt.year}", exist_ok=True)
            # with open(f"data/{board}/{dt.year}/{dt_str}_{article_id}.html", "w+") as f:
            #     f.write(item['html_content'])
        except Exception as e:
            print(e)
            print(f"有問題的文章: {article_id}")
            logger.warning(e)
            logger.warning(f"有問題的文章iD: {article_id}")
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
            # f = open('/data/rawdata/{}.jsonl'.format(board), 'wb')
            f = open('{}.jsonl'.format(board), 'wb')

            exporter = CustomJsonLinesItemExporter(f)
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
        # print(f"====={post_obj['published']}=====")
        # print(item['title'])
        # print("item['count']:\n")
        # print(f"推: {item['count']['推']}  噓: {item['count']['噓']} →: {item['count']['→']}")
        # print("post_obj['upvote']:\n")
        # print(f"{post_obj['upvote']}")

        # self.ptt_col.insert_one(post_obj)
        # self.ptt_col.update_one(
        #     {"id": post_obj["id"]},
        #     {"$set": post_obj},
        #     upsert=True
        # )
        exporter = self._exporter_for_item(item)
        exporter.export_item(post_obj)

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
                # self.ptt_col.update_one(
                #     {"id": comment_obj["id"]},
                #     {"$set": comment_obj},
                #     upsert=True
                # )
                exporter = self._exporter_for_item(item)
                exporter.export_item(comment_obj)
        return item
