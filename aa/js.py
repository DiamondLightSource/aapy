from aa import data, fetcher
import json


class JsonFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(JsonFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.json'.format(self._endpoint)

    def _parse_raw_data(self, raw_data, pv, start, end, count):
        json_data = json.loads(raw_data)
        archive_data = data.ArchiveData.empty(pv)

        if json_data:
            events = []
            json_events = json_data[0]['data']
            for json_event in json_events:
                timestamp = json_event['secs'] + 1e-9 * json_event['nanos']
                events.append(data.ArchiveEvent(pv,
                                                json_event['val'],
                                                timestamp,
                                                json_event['severity']))

            archive_data = data.data_from_events(pv, events)

        return archive_data
