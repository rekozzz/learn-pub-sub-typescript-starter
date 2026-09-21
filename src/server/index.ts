import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);

  const ch = await conn.createConfirmChannel();

  console.log("Connected to RabbitMQ");

  printServerHelp();

  while (true) {
    const words = await getInput();

    if (words.length === 0) {
      continue;
    }

    if (words[0] === "pause") {
      console.log("Sending pause message");

      const state: PlayingState = {
        isPaused: true,
      };

      await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
    } else if (words[0] === "resume") {
      console.log("Sending resume message");

      const state: PlayingState = {
        isPaused: false,
      };

      await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
    } else if (words[0] === "quit") {
      console.log("Exiting...");
      break;
    } else {
      console.log("I don't understand this message");
    }
  }

  process.on("SIGINT", async () => {
    console.log("Shutting down...");

    await conn.close();
    process.exit(0);
  });
}

main();