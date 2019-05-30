from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen


AAAI_URL = 'https://aaai.org/Library/AAAI/aaai{}contents.php'
AUTHOR_URL = ' https://aaai.org/ocs/index.php/AAAI/AAAI{}/rt/bio/{}/0 '

SCHEMA = ['year', 'paper_id', 'title', 'url',
          'author_name', 'university', 'country']


def create_empty_csv():
    aaai_df = pd.DataFrame(columns=SCHEMA)
    aaai_df.to_csv('data/aaai_authors.csv', index=False)


def get_list_papers(year):
    # Get content
    html = urlopen(AAAI_URL.format(year))
    # Parse it with beautiful soup
    content_soup = BeautifulSoup(html, 'html.parser')
    all_papers = content_soup.findAll('p', attrs={'class': 'left'})
    return all_papers


def authors_infos(year, paper_id):
    authors_html = urlopen(AUTHOR_URL.format(year, paper_id))
    authors_soup = BeautifulSoup(authors_html, 'html.parser')
    #print(authors_soup)
    authors_info = authors_soup.findAll('div', attrs={'id': 'author'})
    authors = []
    for author_info in authors_info:
        res = dict()
        res['author_name'] = author_info.find('em').text
        author_location = (author_info.find('p').text).split('\t')
        try:
            res['university'] = author_location[1]
            res['country'] = author_location[2]
        except:
            res['university'] = None
            res['country'] = None
            pass
        authors += [res]
    return authors


def parse_paper(year, paper_tree):
    df = pd.DataFrame(columns=SCHEMA)
    row = dict()
    row['year'] = year
    try:
        row['title'] = paper_tree.find('a').text
    except AttributeError as e:
        return None
    url = paper_tree.find('a').attrs['href']
    row['url'] = url
    if url.split('/')[0] != '..':
        paper_id = url.split('/')[-1]
        row['paper_id'] = paper_id
        authors_info_list = authors_infos(year, paper_id)
        if len(authors_info_list) == 0:
            try:
                authors_str = paper_tree.find('i').text
            except AttributeError as e:
                return None
            authors_split = authors_str.split(',')
            authors_info_list = []
            for author_info in authors_split:
                res = dict()
                res['author_name'] = author_info.strip()
                res['university'] = None
                res['country'] = None
                authors_info_list += [res]

        for author_info in authors_info_list:
            new_row = row.copy()
            new_row.update(author_info)
            row_df = pd.DataFrame([new_row], columns=SCHEMA)
            df = df.append([row_df])
        df.to_csv('data/aaai_authors.csv', mode='a', index=False, header=False)


if __name__ == "__main__":
    create_empty_csv()
    years = ['98', '97', '96', '94', '93', '92', '91', '90',
         '88', '87', '86', '84', '83', '82', '80']

    for year in years:
        papers = get_list_papers(year)
        N = len(papers)
        print('year {} - total papers {}'.format(year, N))
        for i, paper_tree in enumerate(papers):
            parse_paper(year, paper_tree)
            if (round(i / N, 2)*100 % 10 == 0):
                print('{}%'.format(round(i / N, 2)*100))
        print('----ok--{}'.format(year))

