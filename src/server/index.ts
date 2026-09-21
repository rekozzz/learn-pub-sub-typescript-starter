import amqp from "amqplib";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);

  console.log("Connected to RabbitMQ");

  process.on("SIGINT", async () => {
    console.log("Shutting down...");

    await conn.close();
    process.exit(0);
  });
}

main();