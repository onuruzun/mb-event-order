const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer({
  idempotent: false,
  transactionalId: "test-transactional-id",
});

const run = async () => {
  await producer.connect();
  for (let i = 0; i < 20; i++) {
    await producer.send({
      topic: "test-topic",
    //   partition: 3,
      messages: [{ value: `${i}` }],
      key: "create",
    });
  }
};

run().catch(console.error);
