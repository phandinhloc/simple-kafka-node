const kafka = require('kafka-node');
const { kafkaUrl, consumerGroup } = require('./messaging-config');
let logger = require('./logger');

module.exports = {};

let consumer;
const handlerMap = new Map();
const topics = new Set();
const getHandleKey = (topic, eventType) => `${topic}/${eventType}`;

module.exports.init = () => new Promise((resolve, reject) => {
  try {
    logger.info(`listen to kafka at ${kafkaUrl}`)
    consumer = new kafka.ConsumerGroup({
      kafkaHost: kafkaUrl,
      groupId: consumerGroup,
    }, []);

    consumer.on('message', (message) => {
      try {
        givenLogger.debug(`new message ${message}`)
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
          logger.error(error);
      }
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


module.exports.subscribe = (topic, eventType, handler) => new Promise((resolve, reject) => {
  const handleKey = getHandleKey(topic, eventType);
  if (handlerMap.get(handleKey)) {
    // support one handle/topic at this moment.
    reject(new Error(`Duplicated handle on ${handleKey}`));
  }
  handlerMap.set(handleKey, handler);
  if (!topics.has(topic)) {
    logger.info(`subscribe to topic ${topic}`)
    consumer.addTopics([topic], (err, added) => {
      if (err) {
        logger.error(`subscribe to topic ${topic} failure`)
        reject(new Error(err));
      } else {
        logger.info(`subscribe to topic ${topic} successfully`)
        topics.add(topic);
        resolve(added);
      }
    });
    // Set up the timeout
    setTimeout(function() {
           reject(`Timeout when subscribe to topic ${topic}`);
       }, 5000);
  } else {
    resolve();
  }
});
