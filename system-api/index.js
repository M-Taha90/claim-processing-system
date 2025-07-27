const express = require("express");

const app = express();
app.use(express.json());

// async function connect() {
//   const connection = await amqp.connect("amqp://rabbitmq");
//   channel = await connection.createChannel();
//   await channel.assertQueue(QUEUE);
//   console.log("Connected to RabbitMQ");
// }

const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://rabbitmq";
const QUEUE = "claim_queue";
let channel;

async function connectRabbitMQWithRetry() {
  while (true) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue(QUEUE);
      console.log(" Connected to RabbitMQ (API)");
      break;
    } catch (err) {
      console.log(" Waiting for RabbitMQ (API)...", err.message);
      await new Promise(res => setTimeout(res, 5000));
    }
  }
}


app.post("/submit-claim", async (req, res) => {
  const { claimId } = req.body;
  if (!claimId) return res.status(400).json({ error: "claimId is required" });

  channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify({ claimId })));
  console.log("Claim sent:", claimId);
  res.json({ status: "submitted", claimId });
});

app.listen(5000, () => {
  console.log("Claim API running on port 5000");
  connectRabbitMQWithRetry();
});
