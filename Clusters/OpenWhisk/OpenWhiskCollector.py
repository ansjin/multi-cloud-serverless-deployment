#!/usr/bin/env python
import subprocess
import sys
import os
sys.path.append(os.path.abspath('../'))
from Clusters import BaseCollector
from .PrometheusCollector import PrometheusCollector
from .KubernetesCollector import KubernetesCollector
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


class OpenWhiskCollector(BaseCollector):
    def __init__(self, prometheus_url: str = None, kubernetes_prom_url: str = None):
        self.prom_obj = PrometheusCollector(prometheus_url)
        self.kube_prom_obj = KubernetesCollector(kubernetes_prom_url)

    def init(self, prometheus_url: str, kubernetes_prom_url):
        self.prom_obj = PrometheusCollector(prometheus_url)
        self.kube_prom_obj = KubernetesCollector(kubernetes_prom_url)

    async def collect_cold_starts(self, start: int, end: int) -> DataFrame:
        """ Collects cold starts for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'cold_starts'
        """

        query = "increase(openwhisk_action_coldStarts_total[1m])"
        frame = await self.prom_obj.query_prometheus(query, "cold_starts", start, end, True)
        return frame

    async def collect_function_invocations(self, start: int, end: int) -> DataFrame:
        """ Collects function invocations for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "increase(openwhisk_action_activations_total[1m])"
        frame = await self.prom_obj.query_prometheus(query, "invocations", start, end, True)
        #frame = frame[frame["action"]=="myservice-dev-nodeinfo"]
        #print(frame.action.values)
        return frame

    async def collect_function_memory(self, start: int, end: int) -> DataFrame:
        """ Collects function invocations for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'invocations'
        """

        query = "openwhisk_action_memory"
        frame = await self.prom_obj.query_prometheus(query, "memory", start, end, True)
        return frame

    async def collect_function_runtimes(self, start: int, end: int) -> DataFrame:
        """ Collects function runtimes for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'runtime'
        """

        query = "increase(openwhisk_action_duration_seconds_sum[1m])"
        frame = await self.prom_obj.query_prometheus(query, "execution_times", start, end, True)
        return frame

    async def collect_function_initialization_times(self, start: int, end: int) -> DataFrame:
        """ Collects function initialization times for FaaS functions.
        It uses PrometheusCollector's query_prometheus function.
        Args:
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - The result as Dataframe with the columns: 'timestamp', 'action' and 'inittime'
        """

        # TODO: maybe find sth better than average init time; Maybe bucket in combination with coldstarts?
        query = "increase(openwhisk_action_initTime_seconds_sum[1m])"
        frame = await self.prom_obj.query_prometheus(query, "init_time", start, end, True)
        return frame

    def postprocess_relative_to_invocations(self, frame: DataFrame, measurement: str) -> DataFrame:
        """ Divides the values by invocations.
        If values are missing, the last valid value is used.
        If no values exist at all, this function does nothing.
        Args:
            frame:
                DataFrame - The frame that holds the data
            measurement:
                String - name of the measurement that should be processed

        Returns:
            DataFrame - The processed frame
        """
        if measurement in frame.columns:
            if not frame[measurement].empty:
                # Divide and fill
                frame[[measurement]] = frame[[measurement]].divide(frame['invocations'], axis='index')
                frame[[measurement]] = frame[[measurement]].fillna(method='ffill')

        return frame

    async def collect(self, config_object: object, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects function cold starts, invocations, initialization time and runtime for a Target from the configured prometheus instance.

        Args:
            config_object:
                Object - The Target, for which the prometheus should be queried
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end

        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'action' and measurement fields(s)
        """

        # start each worker
        tasks: List[asyncio.Task] = [
            asyncio.create_task(self.collect_cold_starts(start, end)),
            asyncio.create_task(self.collect_function_invocations(start, end)),
            asyncio.create_task(self.collect_function_runtimes(start, end)),
            asyncio.create_task(self.collect_function_initialization_times(start, end)),
            asyncio.create_task(self.collect_function_memory(start, end))
            #asyncio.create_task(self.kube_prom_obj.collect_replicas(start, end))
        ]

        # wait for all workers
        combined_frame = DataFrame()
        try:
            # wait for max 45 seconds
            for result in asyncio.as_completed(tasks, timeout=10.0):
                frame = await result

                if combined_frame.empty:
                    combined_frame = frame
                else:
                    combined_frame = pd.merge(combined_frame, frame, on=['timestamp', 'action'])
                    #print(combined_frame)

            # Divide by invocations & interpolate
            # If no value exists -> just insert empty values
            combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'inittime')
            combined_frame = self.postprocess_relative_to_invocations(combined_frame, 'runtime')

        except Exception as e:
            print("Exception when tyring to query data")
            print(e)
            traceback.print_exc()

        return self.prom_obj.do_frame_postprocessing(combined_frame, cluster_name, "function_usage")