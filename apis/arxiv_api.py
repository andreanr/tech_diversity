import pandas as pd
import requests
import yaml
from xml.etree import ElementTree
from joblib import Parallel, delayed

URL = 'http://export.arxiv.org/api/query'
SCHEMA = ['it','name','author_name', 'author_middle_name', 'author_surname',
          'arxiv_id',  'published', 'title', 'keyword']


def create_empty_csv(keyword):
    arxiv_df = pd.DataFrame(columns=SCHEMA)
    arxiv_df.to_csv('data/arxiv_{}_authors.csv'.format(keyword), index=False)


def batch_queries(keyword, max=10000, by=100):
    Parallel(n_jobs=7, verbose=51)(delayed(search_query_api)(i, by, keyword)
                                       for i in range(1, max+1, by))

    #for i in range(1, max+1, by):
    #    print(i, i + by - 1)
    #    search_query_api(i, by, keyword)

def retry_get(complete_api_url):
    r = None
    for x in range(3):
        try:
            r = requests.get(complete_api_url, stream=True)
            break
        except requests.ConnectionError as e:
            print(e)
            #print('Connection Error, Retrying in 2 seconds...')
            time.sleep(5)
        else:
            raise
    return r


def search_query_api(start, by, keyword):
    end = start + by - 1
    search_query = 'search_query=all:{keyword}'.format(keyword=keyword)
    start_query = 'start={start}'.format(start=start)
    max_results = 'max_results={end}'.format(end=end)
    sorted_arg = 'sortBy=lastUpdatedDate&sortOrder=ascending'
    #
    complete_api_url = '{url}?{search}&{start}&{max}&{sort}'.format(url=URL,
                                                              search=search_query,
                                                              start=start_query,
                                                              max=max_results,
                                                              sort=sorted_arg)
    # Call API
    print(complete_api_url)
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
    df.to_csv('data/arxiv_{}_authors.csv'.format(keyword), mode='a', index=False, header=False)
    return True



if __name__ == "__main__":
    with open("apis/keywords.yaml", 'r') as k:
        try:
            keywords = yaml.load(k)
        except yaml.YAMLError as exc:
            print(exc)

    # Define keyword
    symbols = keywords['Symbols']
    # Create empty csv
    for keyword in symbols:
        create_empty_csv(keyword)
        batch_queries(keyword, max=1000000, by=1000)
    #search_query_api(1, 10, keyword)
