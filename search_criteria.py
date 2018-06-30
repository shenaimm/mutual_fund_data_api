import datetime as dt


class DataCriteria:
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

    def set_schemename(self, schemename):
        self.schemename = schemename
        return self

d = DataCriteria().set_schemename('SBI Bluechip Fund-Growth')