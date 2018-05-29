const kafka = require('kafka-node');
const { kafkaUrl, clientId } = require('./messaging-config');

module.exports = {};

let producer;
module.exports.init = () => new Promise((resolve, reject) => {
  try {
    const client = new kafka.Client(kafkaUrl, clientId);
    producer = new kafka.HighLevelProducer(client);
    resolve();
  } catch (e) {
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

