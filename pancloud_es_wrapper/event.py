# -*- coding: utf-8 -*-

"""Interact with the Application Framework Event Service API via LoggingService.

This library provides similar interface to pancloud.EventService while using
pancloud.LoggingService calls.
"""

import json as json_module
import logging
import time

import cachetools
import pancloud


MAX_CACHE_SIZE = 1024 * 1024
QUERY_OVERLAP = 60
SERIAL_UPDATE_INTERVAL_SECS = 10 * 60
THE_FUTURE = 2**31 - 1  # Maximum timestamp accepted by pancloud API.


class UnsupportedAction(Exception):
    pass


class EventService(object):

    def __init__(self, **kwargs):
        """

        Parameters:
            All parameters are passed to the underlying pancloud.LoggingService and
            pancloud.EventService instances directly.

        Args:
            **kwargs: Supported :class:`~pancloud.httpclient.HTTPClient` parameters.
        """
        self._debug = logging.getLogger(__name__).debug

        # The underlying pancloud services
        self._logging_service = pancloud.LoggingService(**kwargs)
        self._event_service = pancloud.EventService(**kwargs)
        self._event_service.__class__.__name__ = self.__class__.__name__  # used for __repr__

        self._returned_items = cachetools.TTLCache(MAX_CACHE_SIZE, QUERY_OVERLAP * 4)

    def __repr__(self):
        return repr(self._event_service)

    def ack(self, channel_id=None, **kwargs):
        """Send a read acknowledgment to the service.

        Unsupported in this wrapper.
        """
        raise UnsupportedAction()

    def flush(self, channel_id=None, **kwargs):
        """Discard all existing events from the event channel.

        Unsupported in this wrapper.
        """
        raise UnsupportedAction()

    def get_filters(self, channel_id=None, **kwargs):
        """Retrieve the filters currently set on the channel.

        Delegates the call to pancloud.EventService.get_filters.
        """
        return self._event_service.get_filters(channel_id, **kwargs)

    def nack(self, channel_id=None, **kwargs):
        """Send a negative read-acknowledgement to the service.

        Unsupported in this wrapper.
        """
        raise UnsupportedAction()

    def poll(self, channel_id=None, json=None, **kwargs):
        """Read one or more events from a channel.

        Unsupported in this wrapper.
        """
        raise UnsupportedAction()

    def set_filters(self, channel_id=None, json=None, **kwargs):
        """Set one or more filters for the channel.

        Delegates the call to pancloud.EventService.set_filters.
        """
        return self._event_service.set_filters(channel_id, json, **kwargs)

    def xpoll(self, channel_id=None, json=None, ack=False,
              follow=False, pause=None, **kwargs):
        """Retrieve logType, event entries iteratively in a non-greedy manner.

        Generator function to return logType, event entries from poll
        API request.

        Args:
            channel_id (str): The channel ID.
            json (dict): Payload/request body.
            ack (bool): True to acknowledge read.
            follow (bool): True to continue polling after channelId empty.
            pause (float): Seconds to sleep between poll when follow and channelId empty.
            **kwargs: Supported :meth:`~pancloud.httpclient.HTTPClient.request` parameters.

        Yields:
            dictionary with single logType and event entries.
        """
        if ack:
            raise UnsupportedAction("ack=True action is unsupported for this wrapper")

        filters = json_module.loads(self.get_filters(channel_id=channel_id).text)["filters"]
        queries = {}
        for filt in filters:
            ((k, v),) = filt.items()
            if "WHERE" in v["filter"].upper() or "LIMIT" in v["filter"].upper():
                raise UnsupportedAction("This wrapper does not support EventService filters with WHERE clause")
            queries[k] = v["filter"]

        last_serials_update = 0
        serials = set()

        while True:
            if time.monotonic() - SERIAL_UPDATE_INTERVAL_SECS > last_serials_update:
                last_serials_update = time.monotonic()
                serials.update(self._get_serials())
                self._debug("Serials set: %s", serials)

            start_time = int(time.time()) - QUERY_OVERLAP

            returned = False
            for key, query_string in queries.items():
                for serial in serials:
                    q = self._logging_service.query({
                        "query": "%s WHERE serial='%s' LIMIT %s" % (
                            query_string.replace("`", ""), serial, MAX_CACHE_SIZE),
                        "startTime": start_time,
                        "endTime": THE_FUTURE,
                        "maxWaitTime": 0  # no logs in initial response
                    }, **kwargs)

                    q.raise_for_status()

                    for res in self._logging_service.xpoll(q.json()["queryId"], sequence_no=0):
                        if not self._is_new(res["_id"]):
                            continue
                        res["_source"]["serial"] = serial
                        returned = True
                        yield {"logType": key, "event": [res["_source"]]}

            if not returned:
                if not follow:
                    return
                if pause is not None:
                    self._debug('sleep %.2fs', pause)
                    time.sleep(pause)



    def _get_serials(self):
        q = self._logging_service.query({
            "query": "SELECT COUNT(*) FROM panw.trsum GROUP BY serial, device_name WITH LIMIT 50 LIMIT 0",
            "startTime": int(time.time()) - 24 * 60 * 60,  # Last day
            "endTime": THE_FUTURE,
            "maxWaitTime": 0,
        })

        q.raise_for_status()

        for result in self._logging_service.iter_poll(q.json()["queryId"], sequence_no=0):
            try:
                buckets = result.json()["result"]["esResult"]["response"]["result"]["aggregations"]["serial"]["buckets"]
            except KeyError:
                raise pancloud.PanCloudError('no "buckets" in response: %s' % result.json())
            for bucket in buckets:
                yield bucket["key"]

    def _is_new(self, res_id):
        if res_id in self._returned_items:
            return False
        self._returned_items[res_id] = True
        return True
