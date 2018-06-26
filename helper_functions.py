import datetime as dt


class DataCriteria:
    def __init__(self):
        self.tdate = dt.date.today()
        self.fdate = (self.tdate - dt.timedelta(days=89)).strftime('%d-%b-%Y')
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
