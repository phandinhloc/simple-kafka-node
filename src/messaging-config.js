const kafkaUrl = process.env.MESSAGING_HOST || 'localhost:2181';
const clientId = process.env.MESSAGING_CLIENT_ID || 'bidding';
const consumerGroup = process.env.MESSAGING_GROUP || 'bidding';

module.exports = { kafkaUrl, clientId, consumerGroup };
