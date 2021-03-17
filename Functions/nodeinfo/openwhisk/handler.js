'use strict'

// This source code is adapted from the NodeInfo funktion of the 
// OpenFaaS project which is licensed under MIT.
// More details can be found here: https://github.com/openfaas/faas.

let os = require('os');
let fs = require('fs');
let util = require('util');

function main(params) {
    return new Promise(function(resolve, reject) {
        fs.readFile("/proc/cpuinfo", "utf8", (err, data) => {

            if(err){
                reject({payload:  err})
            }else{
                let val = "";
                val += "Hostname: " + data + "\n";
                val += "Platform: " + os.platform() + "\n";
                val += "Arch: " + os.arch() + "\n";
                val += "CPU count: " + os.cpus().length + "\n";

                val += "Uptime: " + os.uptime() + "\n";

                if (params && params.length && params.indexOf("verbose") > -1) {
                    val += util.inspect(os.cpus()) + "\n";
                    val += util.inspect(os.networkInterfaces())+ "\n";
                }
                resolve({payload:  val})
            }
        });

     });
}
module.exports.main = main;
