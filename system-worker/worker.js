// Claim Worker Service
// Developed by: Taha Imran

const amqp = require("amqplib");
const fs = require("fs");
const { Client } = require("minio");

const QUEUE = "claim_queue";

// MinIO client setup
const minioClient = new Client({
  endPoint: "minio",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password",
});

// Save data to MinIO as a JSON file
async function saveToMinIO(claimId, data) {
  try {
    const path = `/tmp/${claimId}.json`;
    fs.writeFileSync(path, JSON.stringify(data));
    console.log(" File written:", path);

    await minioClient.fPutObject("claims-bucket", `${claimId}.json`, path);
    console.log(" Saved to MinIO:", `${claimId}.json`);

    fs.unlinkSync(path);
    console.log("🧹 Temp file cleaned up");
  } catch (err) {
    console.error(" Error saving to MinIO:", err.message);
  }
}

// Retry RabbitMQ connection until available
async function connectRabbitMQWithRetry() {
  while (true) {
    try {
      const conn = await amqp.connect("amqp://rabbitmq");
      const ch = await conn.createChannel();
      await ch.assertQueue(QUEUE);
      console.log(" Connected to RabbitMQ (Worker)");

      ch.consume(QUEUE, async (msg) => {
        if (msg !== null) {
          const { claimId } = JSON.parse(msg.content.toString());
          console.log(" Received:", claimId);

          const data = {
            claimId,
            status: "processed",
            amount: 1000,
            processedAt: new Date().toISOString(),
          };

          await saveToMinIO(claimId, data);
          ch.ack(msg);
        }
      });

      break; // stop retrying after success
    } catch (err) {
      console.log(" Waiting for RabbitMQ (Worker)...", err.message);
      await new Promise(res => setTimeout(res, 5000));
    }
  }
}

connectRabbitMQWithRetry();
