from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen


AAAI_URL = 'https://aaai.org/Library/AAAI/aaai{}contents.php'
AUTHOR_URL = ' https://aaai.org/ocs/index.php/AAAI/AAAI18/rt/bio/{}/0 '

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


def authors_infos(paper_id):
    authors_html = urlopen(AUTHOR_URL.format(paper_id))
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
        authors += [res]
    return authors


def parse_paper(year, paper_tree):
    df = pd.DataFrame(columns=SCHEMA)
    row = dict()
    row['year'] = year
    row['title'] = paper_tree.find('a').text
    url = paper_tree.find('a').attrs['href']
    row['url'] = url
    paper_id = url.split('/')[-1]
    row['paper_id'] = paper_id
    authors_info_list = authors_infos(paper_id)
    for author_info in authors_info_list:
        new_row = row.copy()
        new_row.update(author_info)
        row_df = pd.DataFrame([new_row], columns=SCHEMA)
        df = df.append([row_df])
    df.to_csv('data/aaai_authors.csv', mode='a', index=False, header=False)


if __name__ == "__main__":
    create_empty_csv()
    years = ['18', '17', '16', '15', '14', '13', '12', '11', '10', 
         '08', '07', '06', '05', '04', '02', '00', 
         '99', '98', '97', '96', '94', '93', '92', '91', '90',
         '88', '87', '86', '84', '83', '82', '80']

    for year in years:
        papers = get_list_papers(year)
        print('year {} - total papers {}'.format(year, len(papers)))
        for i, paper_tree in enumerate(papers):
            parse_paper(year, paper_tree)
            print(i)
        print('----ok--{}'.format(year))

