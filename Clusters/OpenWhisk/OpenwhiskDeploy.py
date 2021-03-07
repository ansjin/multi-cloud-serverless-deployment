#!/usr/bin/env python
import subprocess
import sys
import os
sys.path.append(os.path.abspath('../'))
from Clusters import BaseDeployment
import logging
import yaml

logging.basicConfig(filename='Logs/log.log', format='%(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class OpenWhiskDeployment(BaseDeployment):

    async def authentication(self, config_object: object, init_phase_commands: list, path_function: str) -> None:
        """ Asynchronous function which creates authentication
            Args:
                config_object:
                    Object - All required authentication information
                path_function
                    string - path of source code
                init_phase_commands
                    list - list of init commands
            Returns:
                None
        """
        # Step 1: Export authorization
        os.environ["OW_AUTH"] = config_object["ow_auth"]
        os.environ["OW_APIHOST"] = config_object["ow_api_host"]
        os.environ["OW_APIGW_ACCESS_TOKEN"] = config_object["ow_apigw_access_token"]

        # Step 2: npm install
        for command in init_phase_commands:
            bash_command_npm = command
            process = subprocess.Popen(bash_command_npm.split(), cwd=path_function, stdout=subprocess.PIPE)
            output_npm, error_npm = process.communicate()
            for line in output_npm.decode().split("\n"):
                logger.debug(line)

    async def deploy(self, config_object: object, path_function: str) -> str:
        """ Asynchronous function which creates the deployment of the serverless function using serverless framework
            Args:
                config_object:
                    Object - All required authentication information
                path_function
                    string - path of source code
            Returns:
                None
        """
        # Step 1: Export authorization
        await self.authentication(config_object["auth"], config_object["phases"]["init"]["commands"], path_function)

        yaml_data = None
        with open(path_function + 'serverless.yml', 'r') as stream:
            try:
                yaml_data = yaml.safe_load(stream)

            except yaml.YAMLError as exc:
                print(exc)
        with open(path_function + 'serverless.yml', 'w') as stream:
            try:
                yaml_data['provider']['memory'] = config_object["meta"]['memory']
                yaml_data['provider']['timeout'] = config_object["meta"]['timeout']
                yaml_data['service'] = config_object["meta"]['service_name']

                documents = yaml.dump(yaml_data, stream)

            except yaml.YAMLError as exc:
                print(exc)

        # Step 2: Final Deploy
        for command in config_object["phases"]["post_init"]["commands"]:
            process = subprocess.Popen(command.split(), cwd=path_function, stdout=subprocess.PIPE)
            output_remove, error_remove = process.communicate()
            for line in output_remove.decode().split("\n"):
                logger.debug(line)

        return "Deployed"

    async def delete(self, config_object: object, path_function: str) -> str:
        # Step 1: Export authorization

        await self.authentication(config_object["auth"],  config_object["phases"]["init"]["commands"], path_function)

        # Step 2: Final Deploy
        for command in config_object["phases"]["delete"]["commands"]:
            process = subprocess.Popen(command.split(), cwd=path_function, stdout=subprocess.PIPE)
            output_remove, error_remove = process.communicate()
            for line in output_remove.decode().split("\n"):
                logger.debug(line)

        return "Deleted"


