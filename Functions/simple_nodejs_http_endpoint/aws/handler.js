'use strict';

const moment = require('moment-timezone');

module.exports.time = (event, context, callback) => {
    const timezone = context.timezone || 'Europe/London';
    const timestr = moment().tz(timezone).format('HH:MM:ss');

    const response = {
        statusCode: 200,
        body: JSON.stringify({
            message: `The time in ${timezone} is: ${timestr}.`,
        }),
    };
    callback(null, response);

};