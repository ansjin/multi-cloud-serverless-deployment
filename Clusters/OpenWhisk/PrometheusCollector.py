#!/usr/bin/env python
import subprocess
import sys
import os
from Clusters import BaseCollector
import logging
import json
import yaml

logging.basicConfig(filename='Logs/log.log', format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)

from pandas import DataFrame
import pandas as pd
import google.cloud
from google.cloud import monitoring_v3
from google.oauth2 import service_account
from typing import List, Tuple

import traceback
import asyncio
import os
import time
import asyncio
import aiohttp
from aiohttp_requests import requests
from abc import abstractmethod


class PrometheusCollector(BaseCollector):

    def __init__(self, prometheus_url: str):
        """ Collects usage data from the openwhisk cluster
        Args:
            prometheus_url:
                String - the url where we can find the prometheus instnace
        """

        self.prometheus_url = prometheus_url

        self.query_base = "/api/v1/query_range?query="

    def parse_result_to_dataframe(self, measurement_field_name: str, response, multiple_actions: bool = False,
                                  action_field: str = 'action') -> DataFrame:
        """ Parses a result from the prometheus instance to a DataFrame.
        Args:
            measurement_field_name:
                String - Name of the measurement field (=values from query result)
            response:
                Object - Parsed from the prometheus reponse
            multiple_actions:
                Boolean, optional - Indicates if the result contains measurement fields from different actions. Default: False

        Returns:
            DataFrame - A DataFrame with the columns 'timestamp' and measurement_name (and action if multiple_actions is set)
        """

        result = response['data']['result']
        if len(result) == 0:
            return DataFrame()

        frame = DataFrame()
        if multiple_actions:
            frames = []

            for action_result in result:
                new_frame = DataFrame(action_result['values'], columns=['timestamp', measurement_field_name])
                new_frame['action'] = action_result['metric'][action_field]

                # if "kubernetes_pod_name" in action_result['metric'].keys():
                #     new_frame['kubernetes_pod_name'] = action_result['metric']["kubernetes_pod_name"]
                #
                #
                # elif "kubernetes_pod_name" not in action_result['metric'].keys() and "pod" in action_result['metric'].keys():
                #     new_frame['kubernetes_pod_name'] = action_result['metric']["pod"]

                frames.append(new_frame)

            frame = pd.concat(frames, join="inner")
        else:
            frame = DataFrame(result[0]['values'], columns=['timestamp', measurement_field_name])
            # action_result = result[0]
            # if "kubernetes_pod_name" in action_result['metric'].keys():
            #     frame['kubernetes_pod_name'] = action_result['metric']["kubernetes_pod_name"]
            #
            # elif "kubernetes_pod_name" not in action_result['metric'].keys() and "pod" in action_result[
            #     'metric'].keys():
            #     frame['kubernetes_pod_name'] = action_result['metric']["pod"]

        frame[measurement_field_name] = frame[measurement_field_name].apply(lambda v: float(v))

        return frame

    def do_frame_postprocessing(self, frame: DataFrame, target_name: str, measurement_category: str) -> DataFrame:
        """ Performs postprocessing on dataframes.
        These are:
            - apply the target name
            - fill N/A values with 0
            - set timestamp as index
            - set the measurement_category (e.g. system resource, function usage)
        Args:
            frame:
                DataFrame - The frame which should be processed
            target_name:
                String - Name of the target
            measurement_category:
                String - Measurement category

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'target', 'measurement_category' and measurement fields(s) (and 'action' if applicable)
        """
        if frame.empty:
            return frame

        frame.reset_index(inplace=True, drop=True)
        frame.set_index("timestamp", inplace=True)
        frame.index = pd.to_datetime(frame.index, unit='s')
        frame['cluster_name'] = target_name
        frame['measurement_category'] = measurement_category
        # frame.fillna(0, inplace=True)

        return frame

    async def query_prometheus(self, query: str, measurement_field_name: str, start: int, end: int,
                               multiple_actions: bool = False, action_field: str = "action") -> DataFrame:
        """ Queries the configured Prometheus instance with a given query.
        Args:
            query:
                String - The query that should be executed
            measurement_field_name:
                String - Name of the measurement (=values from query result)
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
            multiple_actions:
                Boolean, Optional - Indicates if the query returns measurement fields for multiple actions. Default: False
            action_field:
                String, Optional - The field that carries the action name. Default: "action"

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'target' and measurement_field_name(s) (and 'action' if multiple_actions is set)
        """
        url = self.prometheus_url + self.query_base + query + "&start={}&end={}&step=60".format(start, end)

        prometheus_request = await requests.get(url)

        # get result and parse
        if prometheus_request.status == 200:
            response = await prometheus_request.json()
            return self.parse_result_to_dataframe(measurement_field_name, response, multiple_actions, action_field)

        return DataFrame()

    @abstractmethod
    async def collect(self, config_object: object, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects one or more measurements for a Target from the configured Prometheus instance.

        Args:
            config_object:
                Object - The Target, for which the prometheus should be queried
            cluster_name:
                str - cluster_name

            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end

        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'measurement_category', measurement fields(s) (+ 'action' if applicable)
        """
        pass
