#!/usr/bin/env python
import yaml
import sys, getopt
from typing import List
import traceback
import asyncio

from Clusters import BaseCollector
from Clusters import GCFCollector
from Clusters import OpenWhiskCollector
from Clusters import AWSCollector
from datetime import datetime
from InfluxDBWriter import InfluxDBWriter

functions_meta = []
logs_file = "Logs/log.log"


async def collect_from_clusters(configfile: str, provider: str, influx_db_writer_obj: InfluxDBWriter,
                                cluster_collector_obj: BaseCollector = None,
                                providers_list: list = None, all_clusters: bool = False):
    with open(configfile, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if all_clusters:
                for cluster in data['providers'][provider]:
                    curr_cluster = data['providers'][provider][cluster]
                    dt = datetime.now()
                    seconds = int(dt.strftime('%s'))

                    if "monitoring" in data['providers'][provider][cluster]:
                        cluster_collector_obj.init(data['providers'][provider][cluster]["monitoring"]['openwhisk'],
                                                   data['providers'][provider][cluster]["monitoring"]['kubernetes']
                                                   )
                    df = await cluster_collector_obj.collect(curr_cluster, cluster, seconds - 1*60, seconds)
                    print(df)
                    influx_db_writer_obj.write_dataframe_influxdb(df)

            else:
                for cluster_name in providers_list:
                    for cluster in data['providers'][provider]:
                        curr_cluster = data['providers'][provider][cluster]
                        if cluster_name == cluster:
                            dt = datetime.now()
                            seconds = int(dt.strftime('%s'))
                            if "monitoring" in data['providers'][provider][cluster]:
                                cluster_collector_obj.init(
                                    data['providers'][provider][cluster]["monitoring"]['openwhisk'],
                                    data['providers'][provider][cluster]["monitoring"]['kubernetes']
                                    )
                            df = await cluster_collector_obj.collect(curr_cluster, cluster_name, seconds - 1*60, seconds)
                            influx_db_writer_obj.write_dataframe_influxdb(df)
                            break
        except yaml.YAMLError as exc:
            print(exc)


async def main(argv):
    google_data_collector = GCFCollector()

    ow_data_collector = OpenWhiskCollector()

    aws_data_collector = AWSCollector()

    influx_db_writer_obj = InfluxDBWriter("config.yaml")
    tasks: List[asyncio.Task] = []

    tasks.append(
        asyncio.create_task(
            collect_from_clusters("config.yaml", 'google', influx_db_writer_obj, google_data_collector, [], True)
        )
    )
    tasks.append(
        asyncio.create_task(
            collect_from_clusters("config.yaml", 'openwhisk', influx_db_writer_obj, ow_data_collector, [], True)
        )
    )
    tasks.append(
        asyncio.create_task(
            collect_from_clusters("config.yaml", 'aws', influx_db_writer_obj, aws_data_collector, [], True)
        )
    )
    # wait for all workers
    if len(tasks):
        try:
            await asyncio.wait(tasks)
        except Exception as e:
            print("Exception in main worker loop")
            print(e)
            traceback.print_exc()

        print("All deployment/removal finished")

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
