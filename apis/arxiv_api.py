import time
import pandas as pd
import requests
import yaml
import pdb
from xml.etree import ElementTree
from joblib import Parallel, delayed

URL = 'http://export.arxiv.org/api/query'
SCHEMA = ['it','name','author_name', 'author_middle_name', 'author_surname',
          'arxiv_id',  'published', 'title', 'keyword']


def create_empty_csv():
    arxiv_df = pd.DataFrame(columns=SCHEMA)
    arxiv_df.to_csv('data/arxiv_authors.csv', index=False)


def batch_queries(keyword, max=10000, by=100):
    Parallel(n_jobs=7, verbose=51)(delayed(search_query_api)(i, by, keyword)
                                       for i in range(1, max+1, by))


def retry_get(complete_api_url):
    r = None
    for x in range(3):
        try:
            time.sleep(0.01)
            r = requests.get(complete_api_url, stream=True)
            print(r.raise_for_status())
            return r
            break
        except requests.ConnectionError as e:
            print(e)
            #print('Connection Error, Retrying in 2 seconds...')
            time.sleep(5)
        except requests.HTTPError as h:
            print(h)
            time.sleep(5)
        else:
            raise
    return r


def search_query_api(start, by, keyword):
    end = start + by - 1
    search_query = 'search_query=all:"{keyword}"'.format(keyword=keyword)
    start_query = 'start={start}'.format(start=start)
    max_results = 'max_results={end}'.format(end=by)
    sorted_arg = 'sortBy=lastUpdatedDate&sortOrder=ascending'
    #
    complete_api_url = '{url}?{search}&{start}&{max}&{sort}'.format(url=URL,
                                                              search=search_query,
                                                              start=start_query,
                                                              max=max_results,
                                                              sort=sorted_arg)
    # Call API
    print(complete_api_url)
    pdb.set_trace()
    response = retry_get(complete_api_url)
    #response = requests.get(complete_api_url, stream=True)

    if (response):
        if (response.ok):
            print('retrieving......{}-{}'.format(start, end))
            tree =  ElementTree.fromstring(response.content)
            return parsing_tree(start, tree, keyword)
    else:
        return False
        #return pd.DataFrame(columns=SCHEMA)


def tree_key_fix(s):
    return '{http://www.w3.org/2005/Atom}' + s


def parsing_tree(start, tree, keyword):
    #df = []
    df = pd.DataFrame(columns=SCHEMA)
    for entry in tree.findall(tree_key_fix('entry')):
        row = dict()
        row['it'] = start
        row['arxiv_id'] = entry.find(tree_key_fix('id')).text
        row['published'] = entry.find(tree_key_fix('published')).text
        row['title'] = entry.find(tree_key_fix('title')).text
        #row['category'] = entry.find(tree_key_fix('arxiv:primary_category')).text
        #row['journal'] = entry.find(tree_key_fix('arxiv:journal_ref')).text
        row['keyword'] = keyword
        for author in entry.findall(tree_key_fix('author')):
            author_name = author.find(tree_key_fix('name')).text
            row['name'] = author_name
            #print(author_name)
            author_sp = author_name.split()
            if len(author_sp) == 1:
                row['author_name'] = author_sp[0]
                row['author_surname'] = None
                row['author_middle_name'] = None
            if len(author_sp) == 2:
                row['author_name'] = author_sp[0]
                row['author_surname'] = author_sp[1]
                row['author_middle_name'] = None
            elif len(author_sp) == 3:
                row['author_name'] = author_sp[0]
                row['author_middle_name'] = author_sp[1]
                row['author_surname'] = author_sp[2]
            else:
                row['author_name'] = None
                row['author_surname'] = None
                row['author_middle_name'] = None

            row_df = pd.DataFrame([row], columns=SCHEMA)
            df = df.append([row_df])
    #return df
    df.to_csv('data/arxiv_authors.csv', mode='a', index=False, header=False)
    return True



if __name__ == "__main__":
    # Create empty csv
    create_empty_csv()

    # Open yaml with keywords
    with open("apis/keywords.yaml", 'r') as k:
        try:
            keywords = yaml.load(k)
        except yaml.YAMLError as exc:
            print(exc)

    # Define keyword
    symbols = keywords['Robotics']
    for keyword in symbols:
        batch_queries(keyword, max=100000, by=1000)

    symbols = keywords['Learning']
    for keyword in symbols:
        batch_queries(keyword, max=100000, by=1000)

    symbols = keywords['Symbols']
    for keyword in symbols:
        batch_queries(keyword, max=100000, by=1000)

