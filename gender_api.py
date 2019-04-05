import json
import pandas as pd
from urllib.request import urlopen
from joblib import Parallel, delayed


URL = "https://gender-api.com/get?key=" + API_KEY
NROWS = 10000


def clean_name(s):
    return s.lower().replace(' ', '%20')

def gen_api_call(firstname, middlename, surname, country):
    if firstname:
        call = '&split={}'.format(clean_name(firstname))
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

    wipo_names['call'] = wipo_names.apply(lambda x: gen_api_call(x['firstname'],
                                       x['middlename'],
                                       x['surname'],
                                       x['country']), axis=1)
    wipo_names['results'] = wipo_names['call'].map(call_api)
    wipo_names.to_csv('data/api_results/wipo_{}_{}.csv'.format(start, start + NROWS),
             index=False)
    print(start)

if __name__ == "__main__":
    res = Parallel(n_jobs=-1)(delayed(run_api)(start)
               for start in range(0, 8076420, NROWS))
