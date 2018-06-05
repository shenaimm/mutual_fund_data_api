from data_fetch_functions import mud, get_mf_scheme_data
import time
if __name__ =='__main__':
    for i in range(2):
        print('---------------------------------------------------------------------------------------------------------')
        print ('Number of mutual funds:- {t}').format(t=i)
        g=['ICICI Pru Long Term Plan-RP Growth']*i
        a= time.time()
        t = mud(g, get_mf_scheme_data)
        print('Ths is parallel process:- {f}').format(f = time.time() - a)
        a = time.time()
        for i in g:
            get_mf_scheme_data(i)
        print ('This is for loop:- {f}').format(f = time.time() - a)
        print('---------------------------------------------------------------------------------------------------------')