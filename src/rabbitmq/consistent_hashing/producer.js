const amqp = require("amqplib");
const exchange = "ch_exchange";
const exchange_type = "x-consistent-hash";

async function sendMessages() {
  const conn = await amqp.connect("amqp://localhost");
  const channel = await conn.createChannel();

  await channel.assertExchange(exchange, exchange_type, { durable: true });

  for (let i = 0; i < 50000; i++) {
    const routingKey = i.toString();

    const message = {
      event: i,
      routingKey: routingKey,
    };

    channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(message)));

    console.log(`id: ${i} - ${routingKey} - ${new Date().toISOString()}`);
  }

  setTimeout(() => {
    channel.close();
    conn.close();
  }, 500);
}

(async () => {
  sendMessages();
})();
