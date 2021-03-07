#!/usr/bin/env python
import yaml
import sys, getopt
from typing import List
import traceback
import asyncio

from Clusters import BaseDeployment
from Clusters import OpenWhiskDeployment
from Clusters import GoogleDeployment
from Clusters import AWSDeployment


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

    try:
        arguments, values = getopt.getopt(argv, "hc:ao:g:l:dr", ["help", "configfile=", "all_providers",
                                                                 "ow_providers_list=", "gcf_providers_list=",
                                                                 "aws_providers_list=",
                                                                 "deploy", "remove"])
    except getopt.GetoptError:
        print('main.py -c <configfile path> -a <for all providers> '
              '-o <OW provider_list separated by comma> -g <GCF provider_list separated by comma>  '
              '-l <AWS provider_list separated by comma> '
              '-d <for deploying> -r <for removing>')
        sys.exit(2)

    for current_argument, current_value in arguments:
        if current_argument in ("-h", "--help"):
            print('python3 main.py \n -c <configfile path> \n -a <for all providers> '
                  '\n -o <OW provider_list separated by comma> \n -g <GCF provider_list separated by comma>'
                  '\n -l <AWS provider_list separated by comma> \n -d <for deploying> \n -r <for removing>')
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
    try:
        await asyncio.wait(tasks)
    except Exception as e:
        print("Exception in main worker loop")
        print(e)
        traceback.print_exc()

    print("All workers finished")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
