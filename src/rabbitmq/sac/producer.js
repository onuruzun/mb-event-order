const amqp = require("amqplib");
const exchange = "sac_topic_exchange";
const exchange_type = "topic";

async function sendMessages() {
  const conn = await amqp.connect("amqp://localhost");
  const channel = await conn.createChannel();

  await channel.assertExchange(exchange, exchange_type, { durable: true });

  // const routingKeys = ["create", "update", "delete"];

  for (let i = 0; i < 10; i++) {
    // const routingKey =
    //   routingKeys[Math.floor(Math.random() * routingKeys.length)];
    const routingKey = "create";
    const message = {
      event: i,
      routingKey: routingKey,
      content: i % 2 == 0 ? "error" : "success",
    };

    channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(message)));
    console.log(`id: ${i} - ${routingKey} - ${new Date().toISOString()}`);
  }
}

(async () => {
  await sendMessages();
})();
