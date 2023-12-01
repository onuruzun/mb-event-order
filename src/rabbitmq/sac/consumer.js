const amqp = require("amqplib");

const exchange = "sac_topic_exchange";
const dlExchange = "sac_topic_dl_exchange";
const instanceId = 0;
const queue = `sac_q_${instanceId}`;
const dlQueue = `sac_dl_q_${instanceId}`;
const dlRoutingKey = "deadLetter";
const MAX_RETRY = 3;

async function receiveMessages() {
  const conn = await amqp.connect("amqp://localhost");
  const channel = await conn.createChannel();

  await channel.assertQueue(dlQueue, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": dlExchange,
      "x-dead-letter-routing-key": dlRoutingKey,
      "x-single-active-consumer": true,
      "x-queue-type": "quorum",
    },
  });

  await channel.assertQueue(queue, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": dlExchange,
      "x-dead-letter-routing-key": dlRoutingKey,
      "x-single-active-consumer": true,
      "x-queue-type": "quorum",
    },
  });

  channel.bindQueue(queue, exchange, "create");
  channel.bindQueue(dlQueue, dlExchange, dlRoutingKey);
  channel.prefetch(1);

  channel.consume(
    queue,
    (msg) => {
      if (msg) {
        try {
          const message = JSON.parse(msg.content.toString());
          const time = new Date().toISOString();

          console.log(`received event id: ${message.event} - ${time}`);

          if (message.content.includes("error")) {
            throw new Error("Hata oluştu");
          }

          channel.ack(msg);
        } catch (error) {
          const retries = msg.properties.headers["x-retry-count"] || 0;
          if (retries < MAX_RETRY) {
            channel.sendToQueue(dlQueue, msg.content, {
              persistent: true,
              headers: { "x-retry-count": retries + 1 },
            });
          } else {
            console.log(`Discarding: ${JSON.parse(msg.content).event}`);
          }
          channel.ack(msg);
        }
      }
    },
    { noAck: false, priority: 10 }
  );

  channel.consume(
    dlQueue,
    (msg) => {
      if (msg) {
        const retries = msg.properties.headers["x-retry-count"] || 0;
        const message = JSON.parse(msg.content.toString());
        console.log(`dl queue event id: ${message.event} - Retry: ${retries}`);
        if (retries < MAX_RETRY) {
          channel.sendToQueue(queue, Buffer.from(msg.content), {
            persistent: true,
            headers: { "x-retry-count": retries + 1 },
          });
        } else {
          console.log(
            `Max retries reached, discarding message: ${message.event}`
          );
        }
        channel.ack(msg);
      }
    },
    { noAck: false, priority: 10 }
  );
}

receiveMessages();
