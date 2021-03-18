import sys
import os
sys.path.append(os.path.abspath('../'))
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import pandas as pd

from Clusters import BaseCollector
import logging

logging.basicConfig(filename='Logs/log.log', format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)

from pandas import DataFrame
from typing import List, Tuple

import traceback
import asyncio
import time


class AWSCollector(BaseCollector):

    def __init__(self):

        self.cloudwatch_client = boto3.client('cloudwatch')
        self.metric_namespace = "AWS/Lambda"
        self.period = 60

    def init(self, prometheus_url: str, kubernetes_prom_url):
        pass

    async def get_and_convert_data_frame(self, config_object: object, start: int, end: int, stat_type: str,
                                         feature_col_name: str, save_feature_col_name: str) -> DataFrame:
        """ Get and Convert the timeseries data values to a dataframe .
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
           feature_col_name:
               String - name of the feature column name
        Returns:
           DataFrame - Query result as DataFrame - with columns: 'timestamp', 'action', 'region', 'memory',
           'feature_col_name'
        """
        stats = self.cloudwatch_client.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'metric_data',
                    'Expression': "SEARCH('{" +
                                  self.metric_namespace+",FunctionName} "
                                                        "MetricName=\"" +
                                  feature_col_name+"\"', '"+stat_type+"', "+str(self.period)+")",
                    'ReturnData': True
                },
            ],
            StartTime=start,
            EndTime=end,
            ScanBy='TimestampDescending'
        )
        values = []
        for record in stats['MetricDataResults']:
            for idx, inner_record in enumerate(record['Timestamps']):
                values.append(
                    {"action": record['Label'], "timestamp": inner_record, save_feature_col_name: record['Values'][idx]})

        return DataFrame(values)

    async def collect_invocations(self, config_object: object, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'action', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(config_object, start, end, 'Sum',
                                                          'Invocations', 'invocations')

        return result_df

    async def collect_concurrency_invocations(self, config_object: object, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'action', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(config_object, start, end, 'Sum',
                                                          'ConcurrentExecutions', 'concurrent_invocations')

        return result_df

    async def collect_post_runtime_duration(self, config_object: object, start: int, end: int) -> DataFrame:
        """ Collects the number of active instances for GCF Function.
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'action', 'region', 'memory',
            'active_instances'
        """

        result_df = await self.get_and_convert_data_frame(config_object, start, end, 'Average',
                                                          'PostRuntimeExtensionsDuration', 'post_runtime_duration')

        return result_df

    async def collect_execution_times(self, config_object: object, start: int, end: int) -> DataFrame:
        """ Collects the execution times of AWS Lambda functions.
        Args:
            config_object:
                config_object - The GoogleCloudTarget, for which the collector should use
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'action', 'region', 'memory',
            'execution_times'
        """

        result_df = await self.get_and_convert_data_frame(config_object, start, end, 'Average',
                                                          'Duration', 'execution_times')

        return result_df

    def collect_data_from_logs(self, func_name: str, start: int, end: int):
        client = boto3.client('logs')

        query = "fields @timestamp, @billedDuration, @duration, @ingestionTime, @maxMemoryUsed, @memorySize"

        log_group = '/aws/lambda/' + func_name

        start_query_response = client.start_query(
            logGroupName=log_group,
            startTime=start,
            endTime=end,
            queryString=query,
        )

        query_id = start_query_response['queryId']

        response = None

        while response == None or response['status'] == 'Running':
            print('Waiting for query to complete ...')
            time.sleep(1)
            response = client.get_query_results(
                queryId=query_id
            )

        values = []
        if response["results"]:
            for log in response["results"]:
                value = {
                    "timestamp": "",
                    "billed_duration": "",
                    "mem_usage_mb": "",
                    "memory": ""
                }
                for record in log:

                    if "timestamp" in record['field']:
                        value["timestamp"] = record['value']

                    elif "billedDuration" in record['field']:
                        value["billed_duration"] = float(record['value'])

                    elif "maxMemoryUsed" in record['field']:
                        value["mem_usage_mb"] = float(record['value']) / 10 ** 6

                    elif "memorySize" in record['field']:
                        value["memory"] = float(record['value']) / 10 ** 6

                if value["billed_duration"]:
                    values.append(value)

            df = pd.DataFrame(values)
            df.set_index("timestamp", inplace=True)
            df.index = pd.to_datetime(df.index, unit='ns')
            df = df.resample(str(self.period)+'s').mean()

            df.reset_index(inplace=True)
            df = df.dropna(how='any')
            df.reset_index(inplace=True, drop=True)
            df["action"]= func_name
            return df
        else:
            return pd.DataFrame(values)

    def do_frame_postprocessing(self, frame: DataFrame, cluster_name: str, measurement_category: str) -> DataFrame:
        """ Performs postprocessing on dataframes.
        These are:
            - apply the target name
            - fill N/A values with 0
            - set timestamp as index
            - set the measurement_category (e.g. system resource, function usage)
        Args:
            frame:
                DataFrame - The frame which should be processed
            cluster_name:
                String - Name of the CLUSTER
            measurement_category:
                String - Measurement category

        Returns:
            DataFrame - The processed DataFrame with the columns 'timestamp', 'cluster_name', 'measurement_category' and measurement fields(s) (and 'action' if applicable)
        """
        if frame.empty:
            return frame

        frame.reset_index(inplace=True, drop=True)
        frame.set_index("timestamp", inplace=True)
        frame.index = pd.to_datetime(frame.index, unit='s')
        frame['cluster_name'] = cluster_name
        frame['measurement_category'] = measurement_category
        # frame.fillna(0, inplace=True)

        return frame

    async def collect(self, config_object: object, cluster_name: str, start: int, end: int) -> DataFrame:
        """ Collects function active_instances, network_egress, and execution_times for a GoogleCloudTarget.
        Args:
            config_object:
                object - The Target, for which the prometheus should be queried
            cluster_name:
                String - The name of the cluster
            start:
                Integer - A timestamp, where the query range should start
            end:
                Integer - A timestamp, where the query range should end
        Returns:
            DataFrame - Query result as DataFrame - with columns: 'timestamp', 'target', 'action' and measurement fields(s)
        """
        # start each worker
        tasks: List[asyncio.Task] = [
            asyncio.create_task(self.collect_execution_times(config_object["auth"], start, end)),
            asyncio.create_task(self.collect_invocations(config_object["auth"], start, end)),
            asyncio.create_task(self.collect_concurrency_invocations(config_object["auth"], start, end)),
            asyncio.create_task(self.collect_post_runtime_duration(config_object["auth"], start, end))
        ]

        # wait for all workers
        combined_frame = DataFrame()
        try:
            # wait for max 45 seconds
            for result in asyncio.as_completed(tasks, timeout=45.0):
                frame = await result

                if frame.empty:
                    continue
                elif combined_frame.empty:
                    combined_frame = frame
                else:
                    combined_frame = pd.merge(combined_frame, frame, on=['timestamp', 'action'])

        except Exception as e:
            print("Exception when tyring to query data")
            print(e)
            traceback.print_exc()

        combined_frame['timestamp'] = pd.to_datetime(combined_frame['timestamp'], utc=True)
        for func_name in combined_frame["action"].unique():

            frame = self.collect_data_from_logs(func_name, start, end)
            frame['timestamp'] = pd.to_datetime(frame['timestamp'], utc=True)
            if frame.empty:
                continue
            else:
                combined_frame = pd.merge(combined_frame, frame, on=['timestamp', 'action'])

        updated_df = self.do_frame_postprocessing(combined_frame, cluster_name, "function_usage")
        return updated_df
