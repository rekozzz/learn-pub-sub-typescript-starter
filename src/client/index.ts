import amqp, { type ConfirmChannel } from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type { GameLog } from "../internal/gamelogic/logs.js";



async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  
    const conn = await amqp.connect(rabbitConnString);
     const ch = await conn.createConfirmChannel();
  

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

await subscribeJSON(
  conn,
  ExchangePerilTopic,
  `army_moves.${username}`,
  `army_moves.*`,
  SimpleQueueType.Transient,
  handlerMove(gameState,ch)
);


await subscribeJSON(
  conn,
  ExchangePerilTopic,
  "war",
  `${WarRecognitionsPrefix}.*`,
  SimpleQueueType.Durable,
  handlerWar(gameState, ch),
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
    const move = commandMove(gameState, words);

    await publishJSON(
      ch,
      ExchangePerilTopic,
      `${ArmyMovesPrefix}.${username}`,
      move,
    );

    console.log("Move published successfully");
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

