'use strict';

const moment = require('moment-timezone');

exports.time = (request, response) => {
  const timezone = params.timezone || 'Europe/London';
  const timestr = moment().tz(timezone).format('HH:MM:ss');

  response.status(200).send(`The time in ${timezone} is: ${timestr}.`);
};
