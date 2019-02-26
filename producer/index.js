const express = require('express')
const fs = require('fs');
const app = express();
const parse = require('csv-parse');
const rabbitMQOpen = require('amqplib').connect('amqp://nrkcspju:FZNkmM7QW6aUUjTxK_KhYLKY4oDhgHFc@skunk.rmq.cloudamqp.com/nrkcspju');
require('./collector');
const _ = require('lodash');

const port = 3000;
app.listen(port, () => console.log(`Producer listening on port ${port}!`))

const fileReadFn = (err, data) => {
  const records = parse(data, {
    columns: true,
    delimiter: ';',
    skip_empty_lines: true
  }, processData);
}
fs.readFile('./data.txt', 'utf8', fileReadFn);

function processData(err, data) {
  if(err) return;
  const groupedData = _.groupBy(data, 'Date');
  const totalLength = Object.keys(groupedData).length;
  rabbitMQOpen.then((conn) => conn.createChannel())
  .then((ch) => {
    var q = 'task_queue';
    console.time('taskExecutionTime');
    console.log(`Total Data: ${data.length}`);
    console.log(`Number of Tasks ${totalLength}`);
    _.forEach(groupedData, (value, key) => {
      ch.assertQueue(q, { durable: true });
      ch.sendToQueue(q, new Buffer(JSON.stringify({ key, value, totalLength })), {persistent: true});
    })
  })
}
