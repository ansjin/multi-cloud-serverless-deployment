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
import re
from .PrometheusCollector import PrometheusCollector


class KubernetesCollector(BaseCollector):

    def __init__(self, prometheus_url: str):
        self.prom_obj = PrometheusCollector(prometheus_url)

    async def collect_replicas(self, start: int, end: int) -> DataFrame:
        """ Collects the amount of active replicas per function.
        Args:
            start:
                Integer - timestamp of start
            end:
                Integer - timestamp of end
        Returns:
            DataFrame - a Pandas Dataframe with the result.
        """
        query = "kube_pod_info{pod=~\".*user-events.*\"}"
        frame = await self.prom_obj.query_prometheus(query, "replicas", start, end, True, "pod")

        # map over every "action" -> remove *-guest-
        frame.drop(['action'], axis=1, inplace=True)
        print(frame)
        #frame["action"] = frame["action"].map(lambda e: re.sub(r'^.*?-user-events-', '', e))

        frame = frame.groupby(["kubernetes_pod_name", "timestamp"]).sum()
        frame.reset_index(inplace=True)
        frame.set_index("timestamp", inplace=True)

        return frame