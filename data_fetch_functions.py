import re
import warnings
import socket
from datetime import timedelta
import pickle
import numpy as np
import pandas as pd
import requests as r
from bs4 import BeautifulSoup
from fuzzywuzzy import process, fuzz
import datetime as dt



class MFSearchCriteria:
    def __init__(self):
        self.tdate = dt.date.today()
        self.fdate = (self.tdate - dt.timedelta(days=80)).strftime('%d-%b-%Y')
        self.tdate = dt.date.today().strftime('%d-%b-%Y')

    def set_tdate(self, tdate):
        self.tdate = tdate
        return self

    def set_fdate(self, fdate):
        self.fdate = fdate
        return self

    def set_mfnumber(self, mfnumber):
        self.mfnumber = mfnumber
        return self

    def set_schemenumber(self, schemenumber):
        self.schemenumber = schemenumber
        return self


class MfData:
    def __init__(self):
        pass


class MFException(object):
    '''Exception class to explain the errors and reasons for failures better'''
    def __init__(self):
        self.exception_dict = {'no_internet_connection':Exception('YOU DON\'T HAVE AN ACTIVE INTERNET CONNECTION, I CHECKED BELIEVE ME!!'),
                           'mutual_fund_not_found':Exception('I COULD NOT FIND THE MUTUAL FUND YOU ARE LOOKING FOR!')}
    def error_return(self, type):
        return self.exception_dict[type]


class MFUtils:
    '''Class to keep any utility function'''
    def replace_all(text, dic):
        for i, j in dic.items():
            text = text.replace(i, j)
        return text

    # TODO: try and add more format types
    def date_split(**kwargs):
        fDate = dt.datetime.strptime(kwargs['fdate'], '%d-%b-%Y')
        tDate = dt.datetime.strptime(kwargs['tdate'], '%d-%b-%Y')
        dl = np.array([fDate + timedelta(days=x) for x in range((tDate - fDate).days + 1)])
        no_split = np.ceil(len(dl) * 1.0 / 90)
        dl = [{'tdate': max(i).strftime('%d-%b-%Y'), 'fdate': min(i).strftime('%d-%b-%Y')}
              for i in np.array_split(dl, no_split)]
        return dl


def mf_check_internet_conn(funct):
    def check_internet(*args, **kwargs):
        host = "8.8.8.8"
        port = 53
        timeout = 5
        try:
            socket.setdefaulttimeout(timeout)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
            return funct(*args, **kwargs)
        except Exception:
            raise MFException().error_return('no_internet_connection')
            return False
    return check_internet

class MFHelper:

    def __init__(self):
        pass

    # TODO: check the response code of the incoming request only then let it pass
    @mf_check_internet_conn
    def get_data(url, params=None, headers=None, request_type='get'):
        data = r.get(url=url, params=params) if request_type.lower() == 'get' \
            else r.post(url=url, data=headers, params=params)
        return data


    def get_mfh_data():
        fh_data = MFHelper.get_data(url='https://www.amfiindia.com/net-asset-value/nav-history')
        if fh_data:
            fh_data = BeautifulSoup(fh_data.text, 'lxml')
            fh_data = fh_data.findAll(name='option')
            fh_data = [(i.text, i.attrs['value']) for i in fh_data if 'value' in i.attrs.keys() if
                       (i.attrs['value'].isdigit()) if len(re.findall(string=i.text, pattern='Fund')) > 0]
            fh_data = pd.DataFrame(fh_data)
            fh_data.rename(columns={0: 'mfh_name', 1: 'mfh_id'}, inplace=True)
            return fh_data
        else:
            return None

    @staticmethod
    def get_int_mf_data(mfh_id):
        param = {'ID': mfh_id}
        p_data = MFHelper.get_data(url='https://www.amfiindia.com/modules/NavHistorySchemeNav',
                                   params=param,
                                   headers={'X-Requested-With': 'XMLHttpRequest', 'Content-Length': '55'},
                                   request_type='post').json()
        p_data = pd.DataFrame([{'sch_name': j['Text'].strip(), 'sch_id':j['Value'].strip(), 'mfh_id':param['ID']} for j in p_data])
        return p_data

    @staticmethod
    def lookupcreate(refresh_days=90, force_refresh=False):
        if not force_refresh:
            try:
                refresh_date = pd.read_pickle('lookup_data//lookup.p')
                refresh_date = refresh_date['refresh_date'].max()
                refresh_check = (dt.date.today() - refresh_date).days > refresh_days
                if refresh_check:
                    warnings.warn('\nThe lookup table is stale, refreshing the table. This usually takes less than 1 min.')
            except IOError:
                warnings.warn(
                    '\nThe lookup table has been deleted. Refreshing the table. This usually takes less than 1 min.')
                refresh_check = True

        elif force_refresh | refresh_check:
            mfh_data = MFHelper.get_mfh_data()
            lookup = MFHelper.get_int_mf_data(mfh_data.mfh_id.unique())
            lookup['refresh_date'] = dt.date.today()
            lookup.to_pickle(path='lookup.p', protocol=2)
            mfh_data.to_pickle(path='mfh.p', protocol=2)
        return True

    def get_scheme_data(param):
        schemedata = MFHelper.get_data(url='https://www.amfiindia.com/modules/NavHistoryPeriod',
                                       params=param,
                                       headers={'X-Requested-With': 'XMLHttpRequest', 'Content-Length': '55'},
                                       request_type='post')
        if schemedata:
            schemedata = BeautifulSoup(schemedata.text, 'lxml')
            cols = [j.string for j in schemedata.findAll('th') if j.string is not None][-4:]
            schemedata = [k.string for j in schemedata.findAll(name='tr') for k in j if k.string != '\n'][9:]
            schemedata = [schemedata[4 * j:(4 * j) + 4] for j in range(0, int(len(schemedata) / 4))]
            schemedata = pd.DataFrame(data=schemedata, columns=cols)
            schemedata.assign(mf_id = param['mfID'], sc_id = param['scID'])
        else:
            schemedata = None
        return schemedata.to_dict('records')

class GetData:
    def __init__(self):
        pass

    def get_mf_scheme_number(mf_name, match_cutoff=80, n=3):
        lookup, mfh = pd.read_pickle('lookup_data//lookup.p'), pickle.load(open('lookup_data//mfh.p', 'rb'))
        replace_text = {'(d) ': 'dividend', '(md)': 'monthly dividend',
                        '(wd)': 'weekly dividend', '(dd)': 'daily dividend',
                        '(g)': 'growth', ' pru ': 'prudential', '(rp)': 'retail plan',
                        'dir': 'Direct', '-': ' '}

        mf_name = MFUtils.replace_all(mf_name.lower(), replace_text)
        mfh_best_id = mfh[[i for i in mfh.keys() if i in mf_name][0]]
        mfs_name = process.extractBests(query=mf_name, choices=list(lookup[lookup['mfh_id'] == str(mfh_best_id)].sch_name),
                                        score_cutoff=match_cutoff, limit=n, scorer=fuzz.ratio)
        if len(mfs_name) > 0:
            choices = pd.DataFrame(mfs_name, columns=['sch_name', 'match_score'])
            choices = choices.merge(lookup).sort_values('match_score', ascending=False).drop("refresh_date", axis=1)
            return choices.to_dict('records')
        else:
            raise MFException().error_return('mutual_fund_not_found')


    def get_scheme_data(searchcriteria):
        if hasattr(searchcriteria,'mfnumber') & hasattr(searchcriteria, 'schemenumber'):
            dates = MFUtils.date_split(fdate=searchcriteria.fdate, tdate=searchcriteria.tdate)
            mfdata = MfData()
            f_inp = []
            for date in dates:
                params = {}
                params['mfID'], params['scID'], params['fDate'], params['tDate'] = searchcriteria.mfnumber, \
                                                                                   searchcriteria.schemenumber,\
                                                                                   date['fdate'],\
                                                                                   date['tdate']
                f_inp.extend(MFHelper.get_scheme_data(params))
            mfdata.data, mfdata.fDate, mfdata.tDate, mfdata.scID, mfdata.mfID = f_inp, searchcriteria.fdate, \
                                                                                searchcriteria.tdate, \
                                                                                searchcriteria.schemenumber, \
                                                                                searchcriteria.mfnumber
        return mfdata
# a = GetData.get_mf_scheme_number(mf_name = 'Tata Digital India Fund - Direct - Growth', match_cutoff=10, n=10)
# a = MFSearchCriteria().set_schemenumber('135800').set_mfnumber('25').set_fdate('16-Jan-2018').set_tdate('16-Jun-2018')
# b = GetData.get_scheme_data(a)