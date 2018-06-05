import datetime as dt
import re, time
import socket
from datetime import timedelta
import multiprocessing
import numpy as np
import pandas as pd
import requests as r
from bs4 import BeautifulSoup
from fuzzywuzzy import process

# TODO: create a custom error class -Done
# TODO: add the link for the documentation url in this error

class amf_api_exception(object):
    '''
    Exception class to explain the errors and reasons for failures better
    '''
    def __init__(self):
        self.exception_dict = {'no_internet_connection':Exception('YOU DON\'T HAVE AN ACTIVE INTERNET CONNECTION, I CHECKED BELIEVE ME!!'),
                           'mutual_fund_not_found':Exception('I COULD NOT FIND THE MUTUAL FUND YOU ARE LOOKING FOR! TRY THE mf_match FUNCTION. REFER TO THE DOCUMENTATION FOR MORE AT')}
    def error_return(self, type):
        return self.exception_dict[type]





# TODO: segregate the different functions in classes
# TODO: create an object out of data
# TODO: give that object
# TODO: add a beautiful soup function to check if data is there

class utils(object):
    '''class to keep all the miscellaneous functions used in this repo '''
    def replace_all(text, dic):
        for i, j in dic.iteritems():
            text = text.replace(i, j)
        return text

    def date_split(**kwargs):
        fDate = dt.datetime.strptime(kwargs['fDate'], '%d-%b-%Y')
        tDate = dt.datetime.strptime(kwargs['tDate'], '%d-%b-%Y')
        dl = np.array([fDate + timedelta(days=x) for x in range((tDate - fDate).days + 1)])
        no_split = np.ceil(len(dl) * 1.0 / 90)
        dl = [{'tDate': max(i).strftime('%d-%b-%Y'), 'fDate': min(i).strftime('%d-%b-%Y')}
              for i in np.array_split(dl, no_split)]
        return dl


class amfi_decorator:
    '''class to keep any decorator used in this repo'''
    def check_internet_conn_deco(funct):
        def check_internet(*args, **kwargs):
            host = "8.8.8.8"
            port = 53
            timeout = 5
            try:
                socket.setdefaulttimeout(timeout)
                socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
                return funct(*args, **kwargs)
            except Exception:
                raise amf_api_exception.error_return('no_internet_connection')
                return False
        return check_internet
#WIP


class amfi_fetchdata(object)

# TODO: check the response code of the incoming request only then let it pass
@check_internet_conn_deco
def get_data(url, params=None, headers=None, request_type='get'):
    data = r.get(url=url, params=params) if request_type.lower() == 'get' \
        else r.post(url=url, data=headers, params=params)
    return data


# TODO: try and add more format types



def get_mfh_data():
    fh_data = get_data(url='https://www.amfiindia.com/net-asset-value/nav-history')
    if fh_data:
        fh_data = BeautifulSoup(fh_data.text, 'lxml')
        fh_data = fh_data.findAll(name='option')
        fh_data = [(i.text, i.attrs['value']) for i in fh_data if 'value' in i.attrs.keys() if
                   (i.attrs['value'].isdigit()) if len(re.findall(string=i.text, pattern='Fund')) > 0]
        fh_data = pd.DataFrame(fh_data)
        fh_data.rename(columns={0: 'mutual_fund_house', 1: 'mutual_fund_house_number'}, inplace=True)
        return fh_data
    else:
        return None


def get_int_mf_data(mfh_id):
    params = [mfh_id] if type(mfh_id) == int else mfh_id
    params = [{'ID': i} for i in params]
    mf_umfh = None
    for i in params:
        p_data = get_data(url='https://www.amfiindia.com/modules/NavHistorySchemeNav',
                          params=i,
                          headers={'X-Requested-With': 'XMLHttpRequest', 'Content-Length': '55'},
                          request_type='post').json()
        p_data = [(j['Text'].strip(), j['Value'].strip()) for j in p_data]
        p_data = pd.DataFrame(p_data)
        p_data.rename(columns={0: 'mf_name', 1: 'mf_number'}, inplace=True)
        mf_umfh = p_data if mf_umfh is None else pd.concat([mf_umfh, p_data])
    return mf_umfh


def get_scheme_data(params):
    scheme_d = None
    for i in params:
        schemedata = get_data(url='https://www.amfiindia.com/modules/NavHistoryPeriod',
                              params=i,
                              headers={'X-Requested-With': 'XMLHttpRequest', 'Content-Length': '55'},
                              request_type='post')


        if schemedata:
            schemedata = BeautifulSoup(schemedata.text, 'lxml')
            cols = [j.string for j in schemedata.findAll('th') if j.string is not None][-4:]
            schemedata = [k.string for j in schemedata.findAll(name='tr') for k in j if k.string != '\n'][9:]
            schemedata = [schemedata[4 * j:(4 * j) + 4] for j in xrange(0, len(schemedata) / 4)]
            schemedata = pd.DataFrame(data=schemedata, columns=cols)
            schemedata['mf_id'] = i['mfID']
            schemedata['sc_id'] = i['scID']
            scheme_d = schemedata if scheme_d is None else pd.concat([scheme_d, schemedata], ignore_index=True)
        else:
            continue
    return scheme_d


# TODO: add more methods to search for names
def mf_namematch(name):
    replace_text = {'(d) ': 'dividend', '(md)': 'monthly dividend',
                    '(wd)': 'weekly dividend', '(dd)': 'daily dividend',
                    '(g)': 'growth', ' pru ': 'prudential', '(rp)': 'retail plan',
                    'dir':'Direct'}
    name = replace_all(name.lower(), replace_text)
    mfh_house = get_mfh_data()
    if mfh_house is not None:
        best_match_mfh = str(process.extractOne(name, mfh_house.mutual_fund_house.unique())[0])
        int_fund_data = get_int_mf_data(
            mfh_house.query('mutual_fund_house == @best_match_mfh')['mutual_fund_house_number'])
        mfs_name = process.extractOne(name, choices=int_fund_data.mf_name)[0]
        scheme_id = int(int_fund_data.query('mf_name == @mfs_name')['mf_number'].values[0])
        mfh_id = int(mfh_house.query('mutual_fund_house == @best_match_mfh')['mutual_fund_house_number'])
        return {'mfs_name': mfs_name, 'scheme_id': scheme_id, 'mfh_id': mfh_id}
    else:
        raise amf_api_exception.error_return('mutual_fund_not_found')
        return None


def get_mf_scheme_data(mf_name=None, fDate='01-Jan-2018', tDate='02-Feb-2018'):
    if mf_name:
        best_match = mf_namematch(mf_name)
        if best_match:
            s_df = None
            for i in date_split(fDate=fDate, tDate=tDate):
                param_need = [{'fDate': i['fDate'], 'tDate': i['tDate'], 'mfID': best_match['mfh_id'],
                               'scID': best_match['scheme_id']}]
                s_d = get_scheme_data(param_need)
                s_df = s_d if s_df is None else pd.concat([s_df, s_d], ignore_index=True)
            return s_df
        else:
            return None


def mud(ls,f):
    pool = multiprocessing.Pool(processes = 4)
    rs = pool.map(f, ls)
    return rs

