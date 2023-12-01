const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "test-group",
});

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: "test-topic",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`p: ${partition} - o: ${message.offset} - v: ${message.value.toString()} - ${new Date().toISOString()}`);
    },
  });
};

run().catch(console.error);
