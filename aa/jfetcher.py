import aa
from aa import fetcher
import json
import numpy


class JsonFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(JsonFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.json'.format(self._endpoint)

    def _get_values(self, pv, start, end, count):
        json_string = self._fetch_data(pv, start, end)
        #print(json_string)
        json_data = json.loads(json_string)
        if json_data:
            events = json_data[0]['data']
            array_size = min(count, len(events)) if count is not None else len(events)
            values = numpy.zeros((array_size,))
            timestamps = numpy.zeros((array_size,))
            severities = numpy.zeros((array_size,))
            for i, event in zip(range(array_size), events):
                values[i] = event['val']
                timestamps[i] = event['secs'] + 1e-9 * event['nanos']
                severities[i] = event['severity']
        else:  # no values returned
            values = numpy.array((0,))
            timestamps = numpy.array((0,))
            severities = numpy.array((0,))

        return aa.AaData(pv, values, timestamps, severities)
