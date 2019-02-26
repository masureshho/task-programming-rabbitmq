var amqp = require('amqplib/callback_api');
var _ = require('lodash');
amqp.connect('amqp://nrkcspju:FZNkmM7QW6aUUjTxK_KhYLKY4oDhgHFc@skunk.rmq.cloudamqp.com/nrkcspju', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var q = 'task_queue';
    ch.assertQueue(q, {durable: true});
    ch.prefetch(1);
    console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);
    ch.consume(q, function(msg) {
      const message = JSON.parse(msg.content.toString());
      console.log(`Processing for ${message.key}`);
      const sumofSubMeter = _.reduce(message.value, (sum, value) => {
        return {
          Sub_metering_1: +sum.Sub_metering_1 + +value.Sub_metering_1,
          Sub_metering_2: +sum.Sub_metering_2 + +value.Sub_metering_2,
          Sub_metering_3: +sum.Sub_metering_3 + +value.Sub_metering_3,
        }
      }, {
        Sub_metering_1: 0,
        Sub_metering_2: 0,
        Sub_metering_3: 0
      })
      const totalNumber = message.value.length;
      const average = {
        Sub_metering_1: (sumofSubMeter.Sub_metering_1 / totalNumber).toFixed(2),
        Sub_metering_2: (sumofSubMeter.Sub_metering_2 / totalNumber).toFixed(2),
        Sub_metering_3: (sumofSubMeter.Sub_metering_3 / totalNumber).toFixed(2),
      }
      ch.ack(msg);
      const ackData = {
        date: message.key,
        average,
        totalLength: message.totalLength
      }
      ch.assertQueue('task_completed', {durable: true});
      ch.sendToQueue('task_completed', new Buffer(JSON.stringify(ackData)), {persistent: true});
    }, {noAck: false});
  });
});
