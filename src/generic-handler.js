const producer = require('./producer');

module.exports = {};

module.exports.genericHandler = (handleFunction, errorHandle, eventData, eventKey) =>
  handleFunction(eventData.event.data).then((result) => {
    producer.reportActivity(eventKey, result);
  }).catch((err) => {
    errorHandle(err);
    producer.reportError(eventKey, err, eventData).catch(producerError => {
      errorHandle(producerError);
    });
  });
