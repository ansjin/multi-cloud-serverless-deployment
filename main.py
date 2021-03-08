#!/usr/bin/env python
import yaml
import sys, getopt
from typing import List
import traceback
import asyncio
import json

from Clusters import BaseDeployment
from Clusters import OpenWhiskDeployment
from Clusters import GoogleDeployment
from Clusters import AWSDeployment

functions_meta = []


async def deploy_to_clusters(configfile: str, provider: str, cluster_obj: BaseDeployment = None,
                       providers_list: list = None, all_clusters: bool = False):
    with open(configfile, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if all_clusters:
                for cluster in data['providers'][provider]:
                    curr_cluster = data['providers'][provider][cluster]
                    await cluster_obj.deploy(curr_cluster, curr_cluster['path'])

            else:
                for cluster_name in providers_list:
                    for cluster in data['providers'][provider]:
                        curr_cluster = data['providers'][provider][cluster]
                        if cluster_name == cluster:
                            await cluster_obj.deploy(curr_cluster, curr_cluster['path'])
                            break
        except yaml.YAMLError as exc:
            print(exc)


async def remove_from_clusters(configfile: str, provider: str, cluster_obj: BaseDeployment = None,
                               providers_list: list = None, all_clusters: bool = False):
    with open(configfile, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if all_clusters:
                for cluster in data['providers'][provider]:
                    curr_cluster = data['providers'][provider][cluster]
                    await cluster_obj.delete(curr_cluster, curr_cluster['path'])

            else:
                for cluster_name in providers_list:
                    for cluster in data['providers'][provider]:
                        curr_cluster = data['providers'][provider][cluster]
                        if cluster_name == cluster:
                            await cluster_obj.delete(curr_cluster, curr_cluster['path'])
                            break
        except yaml.YAMLError as exc:
            print(exc)


def get_function_meta(configfile: str, provider: str, providers_list: list = None, all_clusters: bool = False):

    with open(configfile, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            if all_clusters:
                for cluster in data['providers'][provider]:
                    curr_cluster = data['providers'][provider][cluster]

                    with open(curr_cluster['path'] + 'serverless.yml', 'r') as yaml_stream:
                        try:
                            data_serverless_yaml = yaml.safe_load(yaml_stream)

                            for function in data_serverless_yaml["functions"]:
                                if provider == "aws":
                                    function_meta = {
                                        "memory": data_serverless_yaml["provider"]["memorySize"],
                                        "timeout": data_serverless_yaml["provider"]["timeout"],
                                        "endpoint": "",
                                        "provider": provider,
                                        "cluster_name": cluster
                                    }
                                elif provider == "openwhisk":
                                    function_meta = {
                                        "memory": data_serverless_yaml["provider"]["memory"],
                                        "timeout": data_serverless_yaml["provider"]["timeout"],
                                        "cluster_name": cluster,
                                        "endpoint": "https://" + curr_cluster["auth"]["ow_api_host"] +
                                                    "/api/v1/web/guest/default/" +
                                                    data_serverless_yaml['service'] + "-dev-" + function
                                    }
                                elif provider == "google":
                                    function_meta = {
                                        "memory": data_serverless_yaml["provider"]["memorySize"],
                                        "timeout": data_serverless_yaml["provider"]["timeout"],
                                        "cluster_name": cluster,
                                        "endpoint": "https://" + data_serverless_yaml["provider"][
                                            "region"] + "-" +
                                                    data_serverless_yaml["provider"]["project"] +
                                                    ".cloudfunctions.net/" +
                                                    data_serverless_yaml['service'] + "-dev-" + function
                                    }
                                functions_meta.append({function: function_meta})

                        except yaml.YAMLError as exc:
                            print(exc)

            else:
                for cluster_name in providers_list:
                    for cluster in data['providers'][provider]:
                        curr_cluster = data['providers'][provider][cluster]
                        if cluster_name == cluster:
                            with open(curr_cluster['path'] + 'serverless.yml', 'r') as yaml_stream:
                                try:
                                    data_serverless_yaml = yaml.safe_load(yaml_stream)

                                    for function in data_serverless_yaml["functions"]:
                                        if provider == "aws":
                                            function_meta = {
                                                "memory": data_serverless_yaml["provider"]["memorySize"],
                                                "timeout": data_serverless_yaml["provider"]["timeout"],
                                                "endpoint": "",
                                                "provider": provider,
                                                "cluster_name": cluster_name
                                            }
                                        elif provider == "openwhisk":
                                            function_meta = {
                                                "memory": data_serverless_yaml["provider"]["memory"],
                                                "timeout": data_serverless_yaml["provider"]["timeout"],
                                                "cluster_name": cluster_name,
                                                "endpoint": "https://" + curr_cluster["auth"]["ow_api_host"] +
                                                            "/api/v1/web/guest/default/" +
                                                            data_serverless_yaml['service'] + "-dev-" + function
                                            }
                                        elif provider == "google":
                                            function_meta = {
                                                "memory": data_serverless_yaml["provider"]["memorySize"],
                                                "timeout": data_serverless_yaml["provider"]["timeout"],
                                                "cluster_name": cluster_name,
                                                "endpoint": "https://" + data_serverless_yaml["provider"][
                                                    "region"] + "-" +
                                                            data_serverless_yaml["provider"]["project"] +
                                                            ".cloudfunctions.net/" +
                                                            data_serverless_yaml['service'] + "-dev-" + function
                                            }
                                        functions_meta.append({function: function_meta})

                                except yaml.YAMLError as exc:
                                    print(exc)
                            break
            return data['name']
        except yaml.YAMLError as exc:
            print(exc)



async def main(argv):
    openwhisk_obj = OpenWhiskDeployment()
    google_obj = GoogleDeployment()
    aws_obj = AWSDeployment()
    configfile = ''
    all_providers = False
    ow_providers_list = []
    gcf_providers_list = []
    aws_providers_list = []
    deployment = False
    remove = False
    meta = False

    try:
        arguments, values = getopt.getopt(argv, "hc:ao:g:l:drm", ["help", "configfile=", "all_providers",
                                                                 "ow_providers_list=", "gcf_providers_list=",
                                                                 "aws_providers_list=",
                                                                 "deploy", "remove", "get_meta_data"])
    except getopt.GetoptError:
        print('main.py -c <configfile path> -a <for all providers> '
              '-o <OW provider_list separated by comma> -g <GCF provider_list separated by comma>  '
              '-l <AWS provider_list separated by comma> -m <for saving functions meta data in a file>'
              '-d <for deploying> -r <for removing>')
        sys.exit(2)

    for current_argument, current_value in arguments:
        if current_argument in ("-h", "--help"):
            print('python3 main.py \n -c <configfile path> \n -a <for all providers> '
                  '\n -o <OW provider_list separated by comma> \n -g <GCF provider_list separated by comma>'
                  '\n -l <AWS provider_list separated by comma> \n -m <for saving functions meta data in a file> '
                  '\n -d <for deploying> \n -r <for removing>')
        elif current_argument in ("-c", "--configfile"):
            configfile = current_value
        elif current_argument in ("-a", "--all_providers"):
            all_providers = True
        elif current_argument in ("-d", "--deploy"):
            deployment = True
        elif current_argument in ("-r", "--remove"):
            remove = True
        elif current_argument in ("-o", "--ow_providers_list"):
            all_arguments = current_value.split(',')
            ow_providers_list = all_arguments
        elif current_argument in ("-g", "--gcf_providers_list"):
            all_arguments = current_value.split(',')
            gcf_providers_list = all_arguments
        elif current_argument in ("-l", "--aws_providers_list"):
            all_arguments = current_value.split(',')
            aws_providers_list = all_arguments
        elif current_argument in ("-m", "--get_meta_data"):
            meta = True


    tasks: List[asyncio.Task] = []

    if deployment:
        tasks.append(
            asyncio.create_task(
                deploy_to_clusters(configfile, 'openwhisk', openwhisk_obj, ow_providers_list, all_providers)
            )
        )
        tasks.append(
            asyncio.create_task(
                deploy_to_clusters(configfile, 'google', google_obj, gcf_providers_list, all_providers)
            )
        )
        tasks.append(
            asyncio.create_task(
                deploy_to_clusters(configfile, 'aws', aws_obj, aws_providers_list, all_providers)
            )
        )
    elif remove:
        tasks.append(
            asyncio.create_task(
                remove_from_clusters(configfile, 'openwhisk', openwhisk_obj, ow_providers_list, all_providers)
            )
        )
        tasks.append(
            asyncio.create_task(
                remove_from_clusters(configfile, 'google', google_obj, gcf_providers_list, all_providers)
            )
        )
        tasks.append(
            asyncio.create_task(
                remove_from_clusters(configfile, 'aws', aws_obj, aws_providers_list, all_providers)
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

    if meta:
        file_name = get_function_meta(configfile, 'openwhisk', ow_providers_list, all_providers)
        get_function_meta(configfile, 'google', gcf_providers_list, all_providers)
        get_function_meta(configfile, 'aws', aws_providers_list, all_providers)
        d = {}
        for k in functions_meta[0].keys():
            d[k] = tuple(d[k] for d in functions_meta)
        with open('MetaInfo/' + file_name + '.json', 'w') as fp:
            json.dump(d, fp, indent=4)

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
