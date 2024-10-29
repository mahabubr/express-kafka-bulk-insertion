import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "express-kafka-bulk-insertion",
  brokers: ["localhost:9092"],
});

export default kafka;
