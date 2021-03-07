# Multi-Serverless-Deployment [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/ansjin/multi-serverless-deployment/blob/main/LICENSE)


It is a tool for deploying serverless functions to multiple heterogeneous serverless compute platform like 
AWS, OpenWhisk and Google Cloud Functions. This tool is based upon the <a href="https://github.com/serverless/serverless">Serverless Framework</a>.
One can use this tool for deploying the function(s) to more than one platform with a single command.
<p align="center">
<img src="https://github.com/ansjin/multi-serverless-deployment/blob/main/Docs/multi-serverless-archi.png"></img>
</p> 

## Install
1. Firstly, install <a href="https://nodejs.org/en/download/"> npm</a>.
2. Install python packages ``` pip install -r requirements.txt```
3. Install <a href="https://github.com/serverless/serverless">Serverless Framework</a> framework  ``` npm install -g serverless```

## Setup 
1. Clone the repository
2. Rename ```config-sample.yaml``` to ```config.yaml```. 
3. Include the clusters, their authentication and functions information in it. 
For example, below shows the example configuration for OpenWhisk
```yaml
version: 0.1
providers:
  openwhisk:
    ow_cluster1:
      auth:
        ow_auth: <write OW Authentication here>
        ow_api_host: <write OW API Host information here>
        ow_apigw_access_token: hello
      path: ./Functions/nodeinfo/openwhisk/ # <Specify the function path>
      meta: # <Specify the meta information for the function>
        service_name: myservice
        memory: 256
        timeout: 60
      phases: # <The phases command, include here if you want to run something>
        init:
          commands:
            - npm install
        post_init:
          commands:
            - serverless deploy
        delete:
          commands:
            - serverless remove
    ow_cluster2: # <Another OW cluster>
     ...
   aws:
     ...
   google
     ...
 ```
 
 
## Running
1. There are two parts in the tool, one for deployment and other for the deletion.  
Below are the different parameters
 ```
python3 main.py 
 -c <configfile path> 
 -a <for all providers> 
 -o <OW provider_list separated by comma> 
 -g <GCF provider_list separated by comma>
 -l <AWS provider_list separated by comma> 
 -d <for deploying> 
 -r <for removing>
 ```

For example, 
1. deploying only GCF cluster: ``` python3 main.py -c ./config.yaml -g gcf_cluster -d ```
2. Deploying Multiple OW and GCF: ``` python3 main.py -c ./config.yaml -o ow_cluster1, ow_cluster2 -g gcf_cluster -d ```
3. For deploying all clusters: ``` python3 main.py -c ./config.yaml -a -d ```
4. For removing all clusters: ``` python3 main.py -c ./config.yaml -r -d ```


## Help and Contribution

Please add issues if you have a question or found a problem. 

Pull requests are welcome too!

Contributions are most welcome. Please message me if you like the idea and want to contribute. 