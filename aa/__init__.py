
URL = 'cs03r-cs-serv-54.cs.diamond.ac.uk'
PORT = 8080


def get_value_at(pv, date):
    jf = jfetcher.JsonFetcher(URL, PORT)
    jf.get_value_at(pv, date)


def get_values(pv, start_date, end_date=None, count=None):
    jf = jfetcher.JsonFetcher(URL, PORT)
    jf.get_values(pv, start_date, end_date, count)
