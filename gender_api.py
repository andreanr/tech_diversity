import json
import pdb
import pandas as pd
from urllib.request import urlopen
from joblib import Parallel, delayed


API_KEY = 'MuufXFrtYBUGeCWnXQ'
URL = "https://gender-api.com/get?key=" + API_KEY
NROWS = 100

def clean_name(s):
    s = s.encode('ascii', 'ignore').decode('ascii')
    return s.lower().replace(' ', '%20')


def gen_api_long_call(firstname, middlename, surname, country):
    """Generate String for the API call """
    if firstname:
        call = '&name={}'.format(clean_name(firstname))
    else:
        return None
    if middlename:
        call = call + '%20{}'.format(clean_name(middlename))
    if surname:
        call = call + '%20{}'.format(clean_name(surname))
    if country:
        call = call + '&country={}'.format(country)
    return call


def call_api(call):
    """Generate the call"""
    call = URL + call
    response = urlopen(call)
    decoded = response.read().decode('utf-8')
    data = json.loads(decoded)
    return data


def run_api(start):
    wipo_names = pd.read_csv('data/wipo_names.csv', skiprows=start,
            nrows=NROWS, header=None)
    wipo_names.columns = ['id', 'firstname', 'middlename', 'surname', 'gender', 'country', 'state']
    wipo_names = wipo_names.where((pd.notnull(wipo_names)), None)
    wipo_names = wipo_names.dropna(subset=['firstname'])

    wipo_names['call'] = wipo_names.apply(lambda x: gen_api_long_call(x['firstname'],
                                       x['middlename'],
                                       x['surname'],
                                       x['country']), axis=1)
    wipo_names['results'] = wipo_names['call'].map(call_api)
    wipo_names.to_csv('data/api_results/wipo_{}_{}.csv'.format(start, start + NROWS),
             index=False)
    print(start)


def run_api_aaai(start):
    names_df = pd.read_csv('data/clean_name/aaai_first_18_05.csv',
            skiprows=start, nrows=NROWS, header=None)
    names_df.columns = ['id', 'author_name', 'Ofirstnam', 'Omidnam',
            'Osurname', 'country_code']
    names_df = names_df.where((pd.notnull(names_df)), None)
    names_df = names_df.dropna(subset=['author_name'])

    names_df['call'] = names_df.apply(lambda x: gen_api_long_call(x['author_name'],
                                                            None, None,
                                                            x['country_code']),
                                                            axis=1)
    names_df['results'] = names_df['call'].map(call_api)
    names_df.to_csv('data/api_results/aaai_first_18_05__{}_{}.csv'.format(start,
        start + NROWS), index=False)
    print(start)


def run_api_arxiv(start):
    names_df = pd.read_csv('data/clean_name/arxiv_first_sy.csv',
            skiprows=start, nrows=NROWS, header=None)
    names_df.columns = ['id', 'name', 'Ofirstnam', 'Omidnam', 'Osurname']
    names_df = names_df.where((pd.notnull(names_df)), None)
    names_df = names_df.dropna(subset=['Ofirstnam'])

    names_df['call'] = names_df.apply(lambda x: gen_api_long_call(x['Ofirstnam'],
                                                            x['Omidnam'], None,
                                                            None), axis=1)
    names_df['results'] = names_df['call'].map(call_api)
    names_df.to_csv('data/api_results/arxiv_first_sy__{}_{}.csv'.format(start,
        start + NROWS), index=False)
    print(start)


if __name__ == "__main__":
    # To run in parallel:
    res = Parallel(n_jobs=-1)(delayed(run_api_arxiv)(start)
               for start in range(0, 42050, NROWS))
    # As for loop
    #for start in range(0, 42050, NROWS):
    #    print(start)
    #    run_api_arxiv(start)

