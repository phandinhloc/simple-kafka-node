const kafka = require('kafka-node');
const { kafkaUrl, clientId } = require('./messaging-config');
let logger = require('./logger');

module.exports = {};

let producer;
module.exports.init = () => new Promise((resolve, reject) => {
  try {
    logger.info(`Initialize producer at ${kafkaUrl} for client ${clientId}`);
    const client = new kafka.KafkaClient({kafkaHost:kafkaUrl});
    producer = new kafka.HighLevelProducer(client);
    producer.on('ready', function () {
      logger.info('Producer is ready');
      resolve();
    });
    producer.on('error', function (err) {
      console.log('error', err);
    });

    setTimeout(function() {
        reject(new Error('Timeout during initializing producer'));
    }, 5000);


  } catch (e) {
      logger.error(`Initialize producer error ${e}`);
    reject(e);
  }
});

module.exports.sendMessage = (topic, data, key) => new Promise((resolve, reject) => {
  try {
    const payLoad = { topic, messages: Buffer.from(JSON.stringify(data)) };
    if (key) {
      payLoad.key = key;
    }
    producer.send([payLoad], (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  } catch (err) {
    reject(err);
  }
});

module.exports.reportError = (eventKey, error, data) => module.exports.sendMessage('errorLog', { eventKey, error, data });
module.exports.reportActivity = (eventKey, data) => module.exports.sendMessage('activityLog', { eventKey, data });
