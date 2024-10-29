import processMessages from "./bulkProcessor.js";
import kafka from "./kafka.js";

const consumer = kafka.consumer({ groupId: "bulk-insert-group" });

export async function connectConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: "bulk-insert-topic", fromBeginning: true });
  console.log("Kafka Consumer Connected");

  let messages = [];

  consumer.run({
    eachMessage: async ({ message }) => {
      messages.push(JSON.parse(message.value.toString()));
    },
  });

  setInterval(async () => {
    if (messages.length > 0) {
      await processMessages(messages);
      messages = [];
    }
  }, 10000);
}

export async function disconnectConsumer() {
  try {
    await consumer.disconnect();
    console.log("🔥 Kafka Producer Disconnected");
  } catch (error) {
    console.log(error);
  }
}
