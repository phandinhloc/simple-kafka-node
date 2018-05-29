const consumer = require('./consumer');
const producer = require('./producer');
const logger = require('../../config/logger');
const { genericHandler } = require('./generic-handler');
const clientService = require('../../api/client/client-service');

module.exports = {};

module.exports.registerTopics = async () => {
  await consumer.init(['client', 'order']);
  await producer.init();
  await consumer.subscribe('client', 'newClient', (eventData) => {
    logger.info(`NewClient event ${eventData}`);
    return genericHandler(clientService.create, eventData);
  });
  await consumer.subscribe('order', 'newOrder', (eventData) => {
    logger.log(`New accepted bid ${eventData}`);
    // write to websocket
  });
};
