import json
import pdb
import pandas as pd
from urllib.request import urlopen
from joblib import Parallel, delayed


API_KEY = 'bc565ffdbb7740ac'
URL = "http://www.name-prism.com/api_token/eth/json/{}/".format(API_KEY)
NROWS = 100


def clean_name(s):
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.replace('/', '%20')
    return s.lower().replace(' ', '%20')


def call_api(name):
    call = '{}'.format(clean_name(name))
    call = URL + call
    print(call)
    try:
        print('si')
        response = urlopen(call)
        decoded = response.read().decode('utf-8')
        data = json.loads(decoded)
        return data
    except:
        return None

def run_api_aaai(start):
    names_df = pd.read_csv('data/clean_name/aaai_first_18_05.csv',
            skiprows=start, nrows=NROWS, header=None)
    names_df.columns = ['id', 'author_name', 'Ofirstnam', 'Omidnam',
            'Osurname', 'country_code']
    names_df = names_df.where((pd.notnull(names_df)), None)
    names_df = names_df.dropna(subset=['author_name'])
    names_df = names_df.drop_duplicates(subset=['author_name'])

    names_df['results'] = names_df['author_name'].map(call_api)
    names_df.to_csv('data/api_results/aaai_first_18_05__{}_{}.csv'.format(start,
        start + NROWS), index=False)
    print(start)


def run_api_arxiv(start):
    names_df = pd.read_csv('data/clean_name/arxiv_first_sy.csv',
            skiprows=start, nrows=NROWS, header=None)
    names_df.columns = ['id', 'name', 'Ofirstnam', 'Omidnam', 'Osurname']
    names_df = names_df.where((pd.notnull(names_df)), None)
    names_df = names_df.dropna(subset=['name'])
    names_df['results'] = names_df['name'].map(call_api)
    names_df.to_csv('data/api_results/arxiv_first_sy__{}_{}.csv'.format(start,
        start + NROWS), index=False)
    print(start)


def run_api_missing_arxiv(start):
    names_df = pd.read_csv('data/arxiv_missing_gender.csv',
            skiprows=start, nrows=NROWS, header=None)
    names_df.columns = ['name', 'author_name', 'author_middle_name', 'author_surname']
    names_df = names_df.where((pd.notnull(names_df)), None)
    names_df = names_df.dropna(subset=['name'])

    names_df['results'] = names_df['name'].map(call_api2)
    names_df.to_csv('data/api_results/arxiv_missing__{}_{}.csv'.format(start,
        start + NROWS), index=False)
    print(start)


if __name__ == "__main__":
    # To run in parallel
    res = Parallel(n_jobs=-1)(delayed(run_api_arxiv)(start)
               for start in range(0, 42050, NROWS))
    # To run as for loop
    #for start in range(0, 42050, NROWS):
    #    print(start)
    #    run_api_arxiv(start)

