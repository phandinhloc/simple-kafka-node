# simple-kafka-node
An simple interface to work with kafka in nodejs
# Usage
npm install simple-kafka-node

## Message format
```
{
    "type" :"anEventType",
    "data" :"A data object"
}
```

## Configuration
Environment variables
```
MESSAGING_HOST: kafka host, default "locahost:2181"
MESSAGING_CLIENT_ID: client id, default: "bidding"
MESSAGING_GROUP : message group, default: "bidding"
```

## Consumer
```
const {consumer} = require('simple-kafka-node');

await consumer.subscribe('topic1', 'anEventOnTopic1', (eventData) => {
    logger.info(`New event ${eventData}`);
});
await consumer.connect();
```


## Producer
const {producer} = require('simple-kafka-node');
await producer.sendMessage('topic1', data, key);

## Generic consumer handler (a utility that handle event and report error to errorLog if error occurs)

```
const { genericHandler } = require('simple-kafka-node');

const handleFunction = async (data) => {
    // handle data
}
await consumer.subscribe('topic1', 'anEventOnTopic1', (eventData) => {
    return genericHandler(handleFunction, (err) => { logger.error(`${err}`); }, eventData);
  });

 ```
if "handleFunction" has error, a message will be send to "errorLog" topic, else an message is sent to "activityLog" topic
