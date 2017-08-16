from aa import fetcher
import json


class JsonFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(JsonFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.json'.format(self._endpoint)

    def _get_values(self, pv, start, end, count):
        json_string = self._fetch_data(pv, start, end)
        json_data = json.loads(json_string)
        if json_data:
            epics_data = json_data[0]['data']
            # potentially a performance problem here
            if count is not None:
                epics_data = epics_data[:count]
        else:  # no values returned
            epics_data = []
        return epics_data
