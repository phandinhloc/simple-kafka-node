const kafka = require('kafka-node');
const logger = require('../../config/logger');
const { kafkaUrl, consumerGroup } = require('./messaging-config');

module.exports = {};

let consumer;
const handlerMap = new Map();
const topics = new Set();

const getHandleKey = (topic, eventType) => `${topic}/${eventType}`;

module.exports.init = () => new Promise((resolve, reject) => {
  try {
    consumer = new kafka.ConsumerGroup({
      host: kafkaUrl,
      groupId: consumerGroup,
    }, []);

    consumer.on('message', (message) => {
      try {
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
    consumer.addTopics([topic], (err, added) => {
      if (err) {
        reject(new Error(err));
      } else {
        topics.add(topic);
        resolve(added);
      }
    });
  }
  resolve({});
});

