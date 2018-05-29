'use strict';
const consumer = require('./src/consumer');
const producer = require('./src/producer');
const { genericHandler } = require('./src/generic-handler');

module.exports = {consumer, producer, genericHandler}
