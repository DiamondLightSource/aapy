import aa
from aa import fetcher
import json
import numpy


class JsonFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(JsonFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.json'.format(self._endpoint)

    def _parse_raw_data(self, raw_data, pv, start, end, count):
        json_data = json.loads(raw_data)
        if json_data:
            events = json_data[0]['data']
            event_count = min(count, len(events)) if count is not None else len(events)
            try:
                wf_length = len(json_data[0]['data'][0]['val'])
            except TypeError:
                wf_length = 1
            values = numpy.zeros((event_count,))
            timestamps = numpy.zeros((event_count, wf_length))
            severities = numpy.zeros((event_count,))
            for i, event in zip(range(event_count), events):
                values[i] = event['val']
                timestamps[i] = event['secs'] + 1e-9 * event['nanos']
                severities[i] = event['severity']
        else:  # no values returned
            values = numpy.zeros((0,))
            timestamps = numpy.zeros((0,))
            severities = numpy.zeros((0,))

        return aa.data.ArchiveData(pv, values, timestamps, severities)
