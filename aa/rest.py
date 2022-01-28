"""Class for making calls to the Archiver Appliance Rest API."""
import requests

from .utils import MONITOR, SCAN, dict_to_tuples

__all__ = ["AaRestClient"]


class AaRestClient(object):
    """Class used for making calls to the AA Rest API."""

    def __init__(self, hostname, port=80):
        self._hostname = "{}:{}".format(hostname, port)

    def _construct_url(self, command, **kwargs):
        """Construct the appropriate URL for the AA Rest API.

        Args:
            command: AA Rest API command
            kwargs: any parameters used in the URL

        """
        url = "http://{}/mgmt/bpl/{}".format(self._hostname, command)
        if kwargs:
            kwargs = dict_to_tuples(kwargs)
            k, v = kwargs.pop()
            url += "?{}={}".format(k, str(v))
            for k, v in kwargs:
                url += "&{}={}".format(k, str(v))
        return url

    def _rest_get(self, command, **kwargs):
        """Construct appropriate URL and call GET.

        Args:
            command: AA Rest API command
            kwargs: any parameters used in the URL

        Returns:
            parsed JSON objects

        """
        url = self._construct_url(command, **kwargs)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def _rest_post(self, command, payload, headers, **kwargs):
        """Construct and POST payload to appropriate URL.

        Args:
            command: AA Rest API command
            payload: appropriate payload for POST
            headers: HTTP headers including appropriate MIME-TYPE
            kwargs: any parameters used in the URL

        Returns:
            parsed JSON objects

        """
        url = self._construct_url(command, **kwargs)
        response = requests.post(url, payload, headers=headers)
        return response.json()

    def get_all_pvs(self, pv=None, limit=-1):
        """Return a list of PVs that are being archived.

        This includes PVs that have connected in the past but are now
        disconnected (those from get_currently_disconnnected_pvs()),
        but not those that have never connected (those from
        get_never_connected_pvs()).

        Args:
            pv: only return PV names that match pv, which may be a glob
            limit: return a maximum of limit PV names. The default (-1)
                   returns all PV names

        Returns:
            list of PV names

        """
        # Only supply the pv argument if it is not None.
        kwargs = {"limit": limit}
        if pv is not None:
            kwargs["pv"] = pv
        return self._rest_get("getAllPVs", **kwargs)

    def get_pv_type_info(self, pv):
        return self._rest_get("getPVTypeInfo", pv=pv)

    def get_pv_status(self, pv):
        return self._rest_get("getPVStatus", pv=pv)

    def get_pv_statuses(self, pv_names):
        payload = "pv=" + ",".join(pv_names)
        return self._rest_post(
            "getPVStatus",
            payload=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    def get_never_connected_pvs(self):
        pv_info = self._rest_get("getNeverConnectedPVs")
        return [info["pvName"] for info in pv_info]

    def get_currently_disconnected_pvs(self):
        pv_info = self._rest_get("getCurrentlyDisconnectedPVs")
        return set([info["pvName"] for info in pv_info])

    def archive_pv(self, pv, samplingperiod, samplingmethod=SCAN):
        if samplingmethod not in [SCAN, MONITOR]:
            raise ValueError("Sampling method {} not valid".format(samplingmethod))
        return self._rest_get(
            "archivePV",
            pv=pv,
            samplingperiod=samplingperiod,
            samplingmethod=samplingmethod,
        )

    def pause_archiving_pv(self, pv):
        return self._rest_get("pauseArchivingPV", pv=pv)

    def resume_archiving_pv(self, pv):
        return self._rest_get("resumeArchivingPV", pv=pv)

    def delete_pv(self, pv):
        return self._rest_get("deletePV", pv=pv)

    def abort_archiving_pv(self, pv):
        return self._rest_get("abortArchivingPV", pv=pv)

    def change_archival_parameters(self, pv, period, method=MONITOR):
        return self._rest_get(
            "changeArchivalParameters",
            pv=pv,
            samplingperiod=period,
            samplingmethod=method.upper(),
        )

    def get_appliance_metrics(self):
        """Gives summary metrics for all appliances in cluster"""
        return self._rest_get("getApplianceMetrics")

    def get_appliance_metrics_for_appliance(self, appliance):
        """Gives detailed metrics for the appliance on the specified host"""
        return self._rest_get("getApplianceMetricsForAppliance", appliance=appliance)

    def rename_pv(self, pv: str, newname: str):
        """Rename pv to newname. The PV needs to be paused first."""
        return self._rest_get("renamePV", pv=pv, newname=newname)

    def get_policy_list(self):
        """Get the list of available archiving policies

        Note this is not part of the documented BPL API.
        """
        return self._rest_get("getPolicyList")
