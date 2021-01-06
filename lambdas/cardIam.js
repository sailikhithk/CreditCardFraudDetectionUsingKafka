exports.handler = async (event) => {
    // Iterate through keys
    for (let key in event.records) {
      console.log('Key: ', key)
      // Iterate through records
      event.records[key].map((record) => {
        console.log('Record: ', record)
        // Decode base64
        const msg = Buffer.from(record.value, 'base64').toString()
        console.log('Message:', msg)
      }) 
    }
    
    
    
  const kafka = require('kafka-node');
  const bp = require('body-parser');
  
  const kafka_topic = 'CardApproved';
  const Producer = kafka.Producer;
  var KeyedMessage = kafka.KeyedMessage;
  const Client = kafka.Client;
  const client = new kafka.KafkaClient({kafkaHost: 'http://ec2-34-221-95-172.us-west-2.compute.amazonaws.com:8082/topics'});
  
  console.log('client :: '+JSON.stringify(client));
  
  const producer = new Producer(client);
  console.log('about to hit producer code');
  
  
  console.log('Hello there!')
  let message = 'my message';
  let keyedMessage = new KeyedMessage('keyed', 'me keyed message');
  
  producer.send([
    { topic: kafka_topic, partition: 0, messages: [message, keyedMessage], attributes: 0 }
  ], function (err, result) {
    console.log(err || result);
    process.exit();
  });
  
  
  producer.on('error', function (err) {
    console.log('error', err);
  });
  }
  return "success";

