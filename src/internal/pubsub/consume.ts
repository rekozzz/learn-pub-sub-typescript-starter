import amqp from "amqplib";
import type { Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}
export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]>{

   const ch = await conn.createConfirmChannel();

   const isTransiet = queueType === SimpleQueueType.Transient;

  const queue = await ch.assertQueue(queueName, {
  durable: !isTransiet,
  exclusive: isTransiet,
  autoDelete: isTransiet,
  arguments: {
    "x-dead-letter-exchange": "peril_dlx",
  },
});

   ch.bindQueue(queue.queue, exchange, key);    

   return [ch, queue];

}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType,
): Promise<void>{
   const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

  await ch.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
    if(message === null) {
      return;
    }

    const data = JSON.parse(message.content.toString());

   const ackType = handler(data);

   if (ackType === AckType.Ack) {
  console.log("Ack");
  ch.ack(message);
} else if (ackType === AckType.NackRequeue) {
  console.log("NackRequeue");
  ch.nack(message, false, true);
} else if (ackType === AckType.NackDiscard) {
  console.log("NackDiscard");
  ch.nack(message, false, false);
}

});

}