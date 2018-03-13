import json
import utils
import logging
import requests

import aa


def construct_url(hostname, command, **kwargs):
    url = 'http://{}/mgmt/bpl/{}'.format(hostname, command)
    if kwargs:
        k, v = kwargs.popitem()
        url += '?{}={}'.format(k, str(v))
        for k, v in kwargs.items():
            url += '&{}={}'.format(k, str(v))
    return url


def make_rest_call(hostname, command, **kwargs):
    url = construct_url(hostname, command, **kwargs)
    logging.debug('Making call to {}'.format(url))
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


class AaRestClient(object):

    def __init__(self, hostname):
        self._hostname = hostname

    def get_all_pvs(self, limit=-1):
        return make_rest_call(self._hostname, 'getAllPVs', limit=limit)

    def get_pv_info(self, pv_name):
        return make_rest_call(self._hostname, 'getPVTypeInfo', pv=pv_name)

    def get_pv_statuses(self, pv_names):
        url = construct_url(self._hostname, 'getPVStatus')
        payload = 'pv=' + ','.join(pv_names)
        response = requests.post(url, data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})
        return response.json()

    def get_never_connected_pvs(self):
        pv_info = make_rest_call(self._hostname, 'getNeverConnectedPVs')
        return [info['pvName'] for info in pv_info]

    def archive_pv(self, pv, period, method=aa.SCAN):
        if method not in [aa.SCAN, aa.MONITOR]:
            raise ValueError('Sampling method {} not valid'.format(method))
        return make_rest_call(self._hostname, 'archivePV',
                              pv=pv,
                              samplingperiod=period,
                              samplingmethod=method)

    def pause_pv(self, pv):
        return make_rest_call(self._hostname, 'pauseArchivingPV', pv=pv)

    def delete_pv(self, pv):
        return make_rest_call(self._hostname, 'deletePV', pv=pv)

    def abort_request(self, pv):
        return make_rest_call(self._hostname, 'abortArchivingPV', pv=pv)

    def delete_or_abort(self, pv):
        try:
            self.remove_pv(pv)
            logging.info('Deleted {} from AA'.format(pv))
        except requests.exceptions.HTTPError:
            response = self.abort_request(pv)
            if response['status'] == 'ok':
                logging.info('Aborted pending request for {}'.format(pv))
            else:
                logging.warning('PV {} not found in AA'.format(pv))

    def remove_pv(self, pv):
        self.pause_pv(pv)
        self.delete_pv(pv)

    def change_pv(self, pv, period, method=aa.MONITOR):
        return make_rest_call(self._hostname, 'changeArchivalParameters',
                              pv=pv,
                              samplingperiod=period,
                              samplingmethod=method.upper())

    def upload_or_update_pv(self, pv, period, method):
        try:
            info = self.get_pv_info(pv)
            if period != float(info['samplingPeriod']) or method != info['samplingMethod']:
                logging.info('Changing parameters for {}'.format(pv))
                self.change_pv(pv, period, method)
            else:
                logging.debug('Not changing {} - parameters correct'.format(pv))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.info('PV {} not found - adding to AA'.format(pv, e))
                self.archive_pv(pv, period, method)
            else:
                raise e

    def upload_pvs(self, pv_specs):
        specs_list = []
        for spec in pv_specs:
            self.archive_pv(spec.pv, spec.period, spec.method)
            specs_list.append({'pv': spec.pv,
                               # Something in the AA requires these numbers to
                               # be strings, otherwise I don't get a sensible
                               # response.
                               'samplingperiod': str(spec.period),
                               'samplingmethod': spec.method})
        json_list = json.dumps(specs_list)
        print(json_list)
        url = construct_url(self._hostname, 'archivePV')
        print(url)
        req = utils.Request(url, json_list, {'Content-Type': 'application/json'})
        response = json.loads(utils.urlopen(req).read())

        print("response |{}|".format(response))
