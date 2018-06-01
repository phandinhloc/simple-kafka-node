'use strict';
const SimpleConsumer = require('./src/consumer');
const producer = require('./src/producer');
const { genericHandler } = require('./src/generic-handler');

module.exports = {consumer: new SimpleConsumer(), SimpleConsumer, producer, genericHandler}
