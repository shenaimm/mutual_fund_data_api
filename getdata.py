import requests as r
from bs4 import BeautifulSoup
import pandas as pd
import re
from params import *
from helper_functions import *

mfh_data = get_mfh_data(url = url, get_d = False)
params_dict = [{'ID':i} for i in mfh_data['mutual_fund_house_number']]
sc_list =  get_int_mf_data( url_mfh = url_mfh, headers_dict = headers_dict,params = params_dict,get_d = False)
input_scheme_dict = []
fDate = '01-Jan-2018'
tDate = '12-Feb-2018'





#d = get_scheme_data(url =url_scheme, headers = headers_dict, params = input_scheme_dict,get_d = True)

sc_list[sc_list['mfh_id'] == '53']['mf_number'].unique()

for i in sc_list[sc_list['mfh_id'] == '53']['mf_number'].unique():
    input_scheme_dict.extend([{'mfID': 53
                     ,'scID':i,
                     'fDate':fDate,
                     'tDate': tDate}])

d = get_scheme_data(url =url_scheme, headers = headers_dict, params = input_scheme_dict[0:10],get_d = True)





    
