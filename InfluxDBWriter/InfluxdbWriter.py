#!/usr/bin/env python
import yaml
logs_file = "Logs/log.log"

from influxdb import DataFrameClient
class InfluxDBWriter:
    def __init__(self, configfile: str):
        with open(configfile, 'r') as stream:
            try:
                data = yaml.safe_load(stream)

                data = data['influxdb']

                self.client = DataFrameClient(data['hostinfo']['host'], data['hostinfo']['port'],
                                         data['auth']['username'], data['auth']['password'],
                                         data['database']['dbname'])
                self.client.create_database(data['database']['dbname'])

                self.database = data['database']['dbname']

                self.protocol = data['database']['protocol']

            except yaml.YAMLError as exc:
                print(exc)

    def write_dataframe_influxdb(self, df):

        self.client.write_points(df, self.database, protocol=self.protocol)
        print("Writen DataFrame")