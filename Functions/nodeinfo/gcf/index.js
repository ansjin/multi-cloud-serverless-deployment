'use strict'

// This source code is adapted from the NodeInfo funktion of the 
// OpenFaaS project which is licensed under MIT.
// More details can be found here: https://github.com/openfaas/faas.

let os = require('os');
let fs = require('fs');
let util = require('util');

exports.main = (request, response) => {
    fs.readFile("/proc/cpuinfo", "utf8", (err, data) => {

        if(err){
           response.status(400).send(err);
        }else{
            let val = "";
            val += "Hostname: " + data + "\n";
            val += "Platform: " + os.platform() + "\n";
            val += "Arch: " + os.arch() + "\n";
            val += "CPU count: " + os.cpus().length + "\n";

            val += "Uptime: " + os.uptime() + "\n";

            if (request && request.length && request.indexOf("verbose") > -1) {
                val += util.inspect(os.cpus()) + "\n";
                val += util.inspect(os.networkInterfaces())+ "\n";
            }
            response.status(200).send(val);
        }
    });
};
