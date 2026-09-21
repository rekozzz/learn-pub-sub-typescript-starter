import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);

  const ch = await conn.createConfirmChannel();

  console.log("Connected to RabbitMQ");

 const state: PlayingState = {
    isPaused : true,
 }

  await publishJSON(ch, ExchangePerilDirect, PauseKey, state)

  process.on("SIGINT", async () => {
    console.log("Shutting down...");

    await conn.close();
    process.exit(0);
  });
}

main();