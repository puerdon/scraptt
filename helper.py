from jseg import Jieba
import glob
from pyquery import PyQuery
import re
from collections import defaultdict
import json
from datetime import datetime
import argparse
import pathlib
from html.parser import HTMLParser
from multiprocessing import Pool

j = Jieba()

class MLStripper(HTMLParser):
    """HTML tag stripper.

    ref: http://stackoverflow.com/a/925630/1105489
    """

    def __init__(self):  # noqa
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):  # noqa
        self.fed.append(d)

    def get_data(self):  # noqa
        return ''.join(self.fed)

    @classmethod
    def strip_tags(cls, html):  # noqa
        s = cls()
        s.feed(html)
        return s.get_data()

def mod_content(content):
    """Remove unnecessary info from a PTT post."""
    content = MLStripper.strip_tags(content)
    content = re.sub(
        r"※ 發信站.*|※ 文章網址.*|※ 編輯.*", '', content
    ).strip('\r\n-')
    return content

def is_chinese_char(s):
    pattern = re.compile(u'[\u4e00-\u9fa5]')
    return bool(re.match(pattern, s))


def _extract(html):
    pq = PyQuery(html)

    d = defaultdict(lambda: defaultdict(int))

    content = (
        pq('#main-content')
        .clone()
        .children()
        .remove('span[class^="article-meta-"]')
        .remove('div.push')
        .end()
        .html()
    )
    content = mod_content(content)
    msg = content
    qs = re.findall('※ 引述.*|\n: .*', msg)
    for q in qs:
        msg = msg.replace(q, '')
    qs = '\n'.join([i.strip('\n') for i in qs])
    content = msg.strip('\n ')
    try:
        s = j.seg(content, pos=True)
#         print(s)
        for word, pos in s:
            if is_chinese_char(word):
                d[word][pos] += 1
    except:
        print(repr(content))
    
    ######
    comments = []
    for _ in pq('.push').items():
        comment = _('.push-content').text().lstrip(' :')
        if comment.strip() != "":
            try:
                s = j.seg(comment, pos=True)
                for word, pos in s:
                    if is_chinese_char(word):
                        d[word][pos] += 1
            except:
                print(repr(comment))
    return d


def html_to_json(data_dir=None, board_name=None, output_dir=None):

    # 先確保有  json 資料夾
    pathlib.Path(f"{output_dir}/json").mkdir(parents=True, exist_ok=True)
    with open(f"{output_dir}/json/{board_name}.json", "w+") as f:
        d = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        for board_year in glob.glob(f"{data_dir}/{board_name}/*"):
            year = board_year.split('/')[-1]
            for file in glob.glob(f"{board_year}/*"):
                with open(file, "r") as html:
                    html = html.read()
                    try:
                        w = _extract(html)
                    except Exception as e:
                        print(e)
                        print(file)
                        continue
                    for (word, q) in w.items():
                        for (pos, freq) in q.items():
                            d[year][word][pos] += freq

            print(board_year)
        json.dump(d, f, ensure_ascii=False)


def sum_word_token_by_year(json_file_path, year):
    sum_word_token = 0

    with open(json_file_path, "r") as f:
        wordlist = json.load(f)

    try:
        for (_, d) in wordlist[year].items():
            for (pos, freq) in d.items():
                sum_word_token += freq
    except KeyError:
        return 0
    return sum_word_token

def main(boards, x):
    p = Pool()
    result = p.map(x, boards)

def x(board, data_dir, output_dir):
    return html_to_json(data_dir=data_dir, board_name=board, output_dir=output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('command')
    parser.add_argument("--json-path", dest="json_path")
    parser.add_argument("--year", dest="year")
    parser.add_argument("--data-path", dest="data_path")


    # parser.add_argument("command", help="html_to_json or sum_by_year")
    # parser.add_argument("-u", "--user-name", dest="file_path")
    args = parser.parse_args()
    print(args)
    if args.command == "sum_by_year":
        if args.year is None or args.json_path is None:
            raise Exception("need arguments --json-path and --year")
        result = sum_word_token_by_year(args.json_path, args.year)
        print(result)
    elif args.command == "html_to_json":
        if args.data_path is None or args.json_path is None:
            raise Exception("need argument --data-path and --json-path")

        boards = []

        data_path = args.data_path
        output_path = args.json_path

        for b in glob.glob(f"{data_path}/*"):
            b = b.split('/')[-1]
            if b == 'json':
                continue
            else:
                boards.append(b)
        print(boards)

        def x(board):
            return html_to_json(data_dir=data_path, board_name=board, output_dir=output_path)
        main(boards, x)
    else:
        raise Exception("Invalid command name!")
