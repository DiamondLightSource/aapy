import logging
import requests

import aa
from aa import utils


class AaRestClient(object):

    def __init__(self, hostname, port=80):
        self._hostname = '{}:{}'.format(hostname, port)

    def _construct_url(self, command, **kwargs):
        url = 'http://{}/mgmt/bpl/{}'.format(self._hostname, command)
        if kwargs:
            k, v = kwargs.popitem()
            url += '?{}={}'.format(k, str(v))
            for k, v in kwargs.items():
                url += '&{}={}'.format(k, str(v))
        return url

    def _make_rest_call(self, command, **kwargs):
        url = self._construct_url(command, **kwargs)
        response = utils.urlget(url)
        response.raise_for_status()
        return response.json()

    def get_all_pvs(self, limit=-1):
        return self._make_rest_call('getAllPVs', limit=limit)

    def get_pv_info(self, pv_name):
        return self._make_rest_call('getPVTypeInfo', pv=pv_name)

    def get_pv_status(self, pv_name):
        return self._make_rest_call('getPVStatus', pv=pv_name)

    def get_pv_statuses(self, pv_names):
        url = self._construct_url('getPVStatus')
        payload = 'pv=' + ','.join(pv_names)
        response = requests.post(url, data=payload, headers={
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        return response.json()

    def get_never_connected_pvs(self):
        pv_info = self._make_rest_call('getNeverConnectedPVs')
        return [info['pvName'] for info in pv_info]

    def get_disconnected_pvs(self):
        pv_info = self._make_rest_call('getCurrentlyDisconnectedPVs')
        return [info['pvName'] for info in pv_info]

    def archive_pv(self, pv, period, method=aa.SCAN):
        if method not in [aa.SCAN, aa.MONITOR]:
            raise ValueError('Sampling method {} not valid'.format(method))
        return self._make_rest_call(
            'archivePV', pv=pv, samplingperiod=period, samplingmethod=method
        )

    def pause_pv(self, pv):
        return self._make_rest_call('pauseArchivingPV', pv=pv)

    def delete_pv(self, pv):
        return self._make_rest_call('deletePV', pv=pv)

    def abort_request(self, pv):
        return self._make_rest_call('abortArchivingPV', pv=pv)

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
        return self._make_rest_call(
            'changeArchivalParameters',
            pv=pv,
            samplingperiod=period,
            samplingmethod=method.upper()
        )

    def upload_or_update_pv(self, pv, period, method, status_dict=None):
        """Ensure PV is being archived with the specified parameters.

        If status_dict is None, request the current status of the PV.

        If it is not being archived, add it to the AA. If it is being
        archived, check whether the parameters match those supplied. If
        not, change the archival parameters. If the PV has been
        requested but has never connected, do nothing.

        Args:
            pv: PV name
            period: sampling period
            method: sampling method
            status_dict: dict returned by get_pv_status()

        """
        # Note: a previous upload_pvs() method has been removed but is in
        # Git history. There was some subtlety in making the rest calls.
        if status_dict is None:
            status_dict = self.get_pv_status(pv)[0]
        status_string = status_dict['status']
        if status_string == 'Initial sampling':
            # Requested but never connected
            logging.info(
                'PV {} was previously requested but has never connected'.format(pv)
            )
        elif status_string == 'Not being archived':
            logging.info('Adding PV {} to AA'.format(pv))
            self.archive_pv(pv, period, method)
        elif status_string == 'Being archived':
            # Compare sampling period only to 1 d.p.
            current_period = round(float(status_dict['samplingPeriod']), 1)
            current_monitored = status_dict['isMonitored'] == 'true'
            request_monitored = method == aa.MONITOR
            if (period != current_period or current_monitored != request_monitored):
                logging.info('Changing parameters for {}'.format(pv))
                self.change_pv(pv, period, method)
        else:
            logging.warning('Unexpected PV status {}'.format(status_string))
