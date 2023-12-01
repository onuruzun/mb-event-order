const amqp = require("amqplib");

const exchange = "topic_exchange";
const instanceId = process.env.pm_id || 0;
const queueName = `topic_q_${instanceId}`;

async function receiveMessages() {
  const conn = await amqp.connect("amqp://localhost");
  const channel = await conn.createChannel();

  await channel.assertQueue(queueName, {
    durable: true,
    exclusive: false,
  });

  channel.bindQueue(queueName, exchange, "1");
  channel.prefetch(1);

  channel.consume(
    queueName,
    (msg) => {
      if (msg) {
        const messageContent = msg.content.toString();
        const time = new Date().toISOString();

        console.log(`${messageContent} - Received at: ${time}`);
        channel.ack(msg);
      }
    },
    { noAck: false }
  );
}

receiveMessages();
