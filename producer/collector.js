var amqp = require('amqplib/callback_api');

let total = 0;
amqp.connect('amqp://nrkcspju:FZNkmM7QW6aUUjTxK_KhYLKY4oDhgHFc@skunk.rmq.cloudamqp.com/nrkcspju', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var q = 'task_completed';
    ch.assertQueue(q, {durable: true});
    ch.prefetch(1);
    ch.consume(q, function(msg) {
      const message = JSON.parse(msg.content.toString());
      total = total + 1;
      if(message.totalLength === total) {
        console.log('Task has been completed');
        console.timeEnd('taskExecutionTime');
      }
      ch.ack(msg);
    }, {noAck: false});
  });
});
