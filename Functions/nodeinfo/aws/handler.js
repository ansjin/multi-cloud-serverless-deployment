'use strict'

// This source code is adapted from the NodeInfo function of the
// OpenFaaS project which is licensed under MIT.
// More details can be found here: https://github.com/openfaas/faas.

let os = require('os');
let fs = require('fs');
let util = require('util');

module.exports.main = (event, context, callback) => {
    fs.readFile("/proc/cpuinfo", "utf8", (err, data) => {

        if (err) {
            const response = {
                statusCode: 404,
                body: JSON.stringify({
                    message: `Error ${err}.`,
                }),
            };

            callback(null, response);
        } else {
            let val = "";
            val += "Hostname: " + data + "\n";
            val += "Platform: " + os.platform() + "\n";
            val += "Arch: " + os.arch() + "\n";
            val += "CPU count: " + os.cpus().length + "\n";

            val += "Uptime: " + os.uptime() + "\n";

            if (context && context.length && context.indexOf("verbose") > -1) {
                val += util.inspect(os.cpus()) + "\n";
                val += util.inspect(os.networkInterfaces()) + "\n";
            }
            const response = {
                statusCode: 200,
                body: JSON.stringify({
                    message: `Payload: ${val}.`,
                }),
            };
            callback(null, response);
        }

    });
};
