const kafka = require('kafka-node');
const { kafkaUrl, consumerGroup } = require('./messaging-config');
let logger = require('./logger');

module.exports = {};

let consumer;
const handlerMap = new Map();
const topics = new Set();
const getHandleKey = (topic, eventType) => `${topic}/${eventType}`;

module.exports.connect = () => new Promise((resolve, reject) => {
  try {
    logger.info(`listen to kafka at ${kafkaUrl}`)
    consumer = new kafka.ConsumerGroup({
      kafkaHost: kafkaUrl,
      groupId: consumerGroup,
    }, [...topics]);

    consumer.on('message', (message) => {
      try {
        logger.debug(`new message ${message}`)
        const { topic } = message;
        const event = JSON.parse(message.value);
        const eventKey = getHandleKey(topic, event.type);
        const handler = handlerMap.get(eventKey);
        if (handler) {
          handler({ event, handleKey: eventKey, topic });
        } else {
          logger.info(`Not interesting event ${eventKey}`);
        }
      } catch (error) {
          logger.error(`Unable to handle event ${message}`);
          logger.error(`Error ${error}`);
      }
      // Set up the timeout
      setTimeout(function() {
           reject(new Error('Timeout during initializing consumer'));
       }, 5000);
    });

    consumer.on('connect', function () {
      logger.info('Consumer connects successfully');
      resolve();
    });

    consumer.on('error', (err) => {
      // TODO
      logger.error(`Consumer error ${err}`);
    });

    resolve();
  } catch (err) {
    reject(err);
  }
});


module.exports.subscribe = (topic, eventType, handler) => {
  const handleKey = getHandleKey(topic, eventType);
  if (handlerMap.get(handleKey)) {
    // support one handle/topic at this moment.
    throw new Error(`Duplicated handle on ${handleKey}`);
  }
  handlerMap.set(handleKey, handler);
  topics.add(topic);
};
