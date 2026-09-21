import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  
    const conn = await amqp.connect(rabbitConnString);
  

    const username = await clientWelcome();
  
    const [ch, queue] = await declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient)
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
