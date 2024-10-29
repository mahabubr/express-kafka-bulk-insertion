import kafka from "./kafka.js";

const producer = kafka.producer();

export async function connectProducer() {
  await producer.connect();
  console.log("🔥 Kafka Producer Connected");
}

export async function sendMessage(topic, message) {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });
  } catch (error) {
    console.log(error);
  }
}

export async function disconnectProducer() {
  try {
    await producer.disconnect();
    console.log("🔥 Kafka Producer Disconnected");
  } catch (error) {
    console.log(error);
  }
}
