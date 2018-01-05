import aa
from aa import fetcher, utils
import json
import numpy
import numbers


def determine_json_type(json_object):
    """Determine the type of the object returned as JSON.

    A scalar is treated as a waveform of length 1.

    Args:
        json_object:

    Returns:
        (Numpy dtype, waveform length)

    """
    try:
        if isinstance(json_object, utils.string23):
            wf_length = 1
        else:
            wf_length = len(json_object)
        datapoint = json_object[0]
    except TypeError:
        wf_length = 1
        datapoint = json_object

    type_mapping = {
        utils.string23: numpy.dtype('U100'),
        numbers.Real: numpy.float64,
        bool: numpy.bool_
    }
    for t in type_mapping:
        if isinstance(datapoint, t):
            print('returning type mapping {}'.format(type_mapping[t]))
            return type_mapping[t], wf_length

    raise Exception('json type {} not understood'.format(datapoint))


class JsonFetcher(fetcher.AaFetcher):

    def __init__(self, hostname, port):
        super(JsonFetcher, self).__init__(hostname, port)
        self._url = '{}/retrieval/data/getData.json'.format(self._endpoint)

    def _parse_raw_data(self, raw_data, pv, start, end, count):
        json_data = json.loads(raw_data)
        if json_data:
            events = json_data[0]['data']
            n_events = len(events)
            dtype, wf_length = determine_json_type(json_data[0]['data'][0]['val'])
            event_count = min(count, n_events) if count is not None else n_events
            values = numpy.zeros((event_count, wf_length), dtype=dtype)
            timestamps = numpy.zeros((event_count,))
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
