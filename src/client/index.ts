import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause } from "./handlers.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  
    const conn = await amqp.connect(rabbitConnString);
  

    const username = await clientWelcome();
  

    const gameState = new GameState(username);

await subscribeJSON(
  conn,
  ExchangePerilDirect,
  `pause.${username}`,
  PauseKey,
  SimpleQueueType.Transient,
  handlerPause(gameState)
);

    while (true) {
  const words = await getInput();

  if (words.length === 0) {
    continue;
  }

  const command = words[0];

  if (command === "spawn") {
    try {
      const unit = commandSpawn(gameState, words);
      console.log(unit);
    } catch (err) {
      if (err instanceof Error) {
        console.log(err.message);
      }
    }
  } else if (command === "move") {
    try {
      commandMove(gameState, words);
      console.log("Unit moved successfully");
    } catch (err) {
      if (err instanceof Error) {
        console.log(err.message);
      }
    }
  } else if (command === "status") {
    commandStatus(gameState);
  } else if (command === "help") {
    printClientHelp();
  } else if (command === "spam") {
    console.log("Spamming not allowed yet!");
  } else if (command === "quit") {
    printQuit();
    break;
  } else {
    console.log("Unknown command");
  }
}
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

