import express from "express";
import {
  connectProducer,
  disconnectProducer,
  sendMessage,
} from "./kafka/producer.js";
import { connectConsumer, disconnectConsumer } from "./kafka/consumer.js";

const app = express();

const port = 3000;

connectProducer();
connectConsumer();

app.use(express.json());

app.post("/bulk-insert", async (req, res) => {
  const data = req.body;

  try {
    await sendMessage("bulk-insert-topic", data);
    res
      .status(200)
      .json({ message: "Data received and queued for bulk insertion." });
  } catch (error) {
    console.log(error);
  }
});

process.on("SIGINT", async () => {
  console.log("\nShutting down gracefully...");
  await disconnectProducer();
  await disconnectConsumer();
});

app.listen(port, () => {
  console.log(`Kafka app listening on port ${port}!`);
});
