import requests as r
import pandas as pd
from bs4 import BeautifulSoup
import lxml, re, logging

logging.basicConfig(filename='mf.log', level=logging.DEBUG)

mf_logger = logging.getLogger(__name__)


def get_data(url, params=None, headers=None, request_type='get'):
    data = r.get(url=url, params=params) if request_type.lower() == 'get' \
        else r.post(url=url, data=headers, params=params)
    return data


def get_mfh_data(url):
    fh_data = get_data(url=url)
    fh_data = BeautifulSoup(fh_data.text, 'html.parser')
    fh_data = fh_data.findAll(name='option')
    fh_data = [(i.text, i.attrs['value']) for i in fh_data if 'value' in i.attrs.keys() if (i.attrs['value'].isdigit()) if len(re.findall(string=i.text, pattern='Fund')) > 0]
    fh_data = pd.DataFrame(fh_data)
    fh_data.rename(columns={0: 'mutual_fund_house', 1: 'mutual_fund_house_number'}, inplace=True)
    return (fh_data)


def get_int_mf_data(url_mfh, headers_dict, params):
    mf_umfh = None
    for i in params:
        p_data = get_data(url=url_mfh, params=i, headers=headers_dict, request_type='post').json()
        p_data = [(j['Text'].strip(), j['Value'].strip()) for j in p_data]
        p_data = pd.DataFrame(p_data)
        p_data.rename(columns={0: 'mf_name', 1: 'mf_number'}, inplace=True)
        mf_umfh = p_data if mf_umfh  else pd.concat([mf_umfh, p_data])
    return (mf_umfh)


def get_scheme_data(url, params, headers):
    scheme_d = None
    for i in params:
        schemedata = get_data(url=url, params=i, headers=headers, request_type='post')
        schemedata = BeautifulSoup(schemedata.text, 'lxml')
        cols = [j.string for j in schemedata.findAll('th') if j.string != None][-4:]
        schemedata = [k.string for j in schemedata.findAll(name='tr') for k in j if k.string != '\n'][9:]
        schemedata = [schemedata[4 * j:(4 * j) + 4] for j in xrange(0, len(schemedata) / 4)]
        schemedata = pd.DataFrame(data=schemedata, columns=cols)
        schemedata['mf_id'] = i['mfID']
        schemedata['sc_id'] = i['scID']
        scheme_d = schemedata if scheme_d is None else pd.concat([scheme_d, schemedata], ignore_index=True)
    return (scheme_d)

    