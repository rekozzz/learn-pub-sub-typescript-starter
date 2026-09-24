import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");

     return AckType.Ack;
  };
}


export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const outcome = handleMove(gs, move);

    process.stdout.write("> ");

    if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };

      await publishJSON(
        ch,
        ExchangePerilTopic,
        `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`,
        rw,
      );

      return AckType.NackRequeue;
    }

    if (outcome === MoveOutcome.Safe) {
      return AckType.Ack;
    }

    return AckType.NackDiscard;
  };
}



export function handlerWar(
  gs: GameState,
): (rw: RecognitionOfWar) => AckType {
  return (rw: RecognitionOfWar) => {
    const resolution = handleWar(gs, rw);

    if (resolution.result === WarOutcome.NotInvolved) {
      process.stdout.write("> ");
      return AckType.NackRequeue;
    }

    if (resolution.result === WarOutcome.NoUnits) {
      process.stdout.write("> ");
      return AckType.NackDiscard;
    }

    if (
      resolution.result === WarOutcome.OpponentWon ||
      resolution.result === WarOutcome.YouWon ||
      resolution.result === WarOutcome.Draw
    ) {
      process.stdout.write("> ");
      return AckType.Ack;
    }

    console.error("Unknown war outcome");
    process.stdout.write("> ");
    return AckType.NackDiscard;
  };
}