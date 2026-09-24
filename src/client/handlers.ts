import type { ConfirmChannel } from "amqplib";

import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";

import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";

import {
  ExchangePerilTopic,
  GameLogSlug,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";


export function handlerPause(
  gs: GameState,
): (ps: PlayingState) => AckType {
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

    if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };

      try {
        await publishJSON(
          ch,
          ExchangePerilTopic,
          `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`,
          rw,
        );

        process.stdout.write("> ");

        return AckType.Ack;
      } catch (err) {
        console.error("Failed to publish war:", err);

        process.stdout.write("> ");

        return AckType.NackRequeue;
      }
    }

    process.stdout.write("> ");

    if (outcome === MoveOutcome.Safe) {
      return AckType.Ack;
    }

    return AckType.NackDiscard;
  };
}


function publishGameLog(
  ch: ConfirmChannel,
  username: string,
  message: string,
): Promise<void> {
  const gameLog: GameLog = {
    username,
    message,
    currentTime: new Date(),
  };

  return publishMsgPack(
    ch,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog,
  );
}


export function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    console.log("PUBLISHING GAME LOG");
    const resolution = handleWar(gs, rw);

    if (resolution.result === WarOutcome.NotInvolved) {
      process.stdout.write("> ");
      return AckType.NackRequeue;
    }

    if (resolution.result === WarOutcome.NoUnits) {
      process.stdout.write("> ");
      return AckType.NackDiscard;
    }

    if (resolution.result === WarOutcome.OpponentWon) {
      const message = `${resolution.winner} won a war against ${resolution.loser}`;

      try {
        await publishGameLog(
          ch,
          rw.attacker.username,
          message,
        );

        process.stdout.write("> ");

        return AckType.Ack;
      } catch (err) {
        console.error("Failed to publish game log:", err);
         console.error(err);

        process.stdout.write("> ");

        return AckType.NackRequeue;
      }
    }

    if (resolution.result === WarOutcome.YouWon) {
      const message = `${resolution.winner} won a war against ${resolution.loser}`;

      try {
        await publishGameLog(
          ch,
          rw.attacker.username,
          message,
        );

        process.stdout.write("> ");

        return AckType.Ack;
      } catch (err) {
        console.error("Failed to publish game log:", err);

        process.stdout.write("> ");

        return AckType.NackRequeue;
      }
    }

    if (resolution.result === WarOutcome.Draw) {
      const message =
        `A war between ${resolution.attacker} and ${resolution.defender} resulted in a draw`;

      try {
        await publishGameLog(
          ch,
          rw.attacker.username,
          message,
        );

        process.stdout.write("> ");

        return AckType.Ack;
      } catch (err) {
        console.error("Failed to publish game log:", err);

        process.stdout.write("> ");

        return AckType.NackRequeue;
      }
    }

    console.error("Unknown war outcome");

    process.stdout.write("> ");

    return AckType.NackDiscard;
  };
}