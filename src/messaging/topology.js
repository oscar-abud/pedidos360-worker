import { EXCHANGES, HEADERS, QUEUES, RETRY_QUEUES } from "../config/messaging.js";

// Declaración de exchanges, colas y bindings (equivalente a los @Bean Queue,
// Exchange y Binding de Spring). Todas las operaciones son idempotentes: si
// ya existen con los mismos argumentos, RabbitMQ no hace nada.

async function declareExchanges(ch) {
  // El DLX va primero: es el alternate-exchange de pedidos.events.
  for (const exchange of [EXCHANGES.DLX, EXCHANGES.EVENTS, EXCHANGES.RETRY, EXCHANGES.RETRY_RETURN]) {
    await ch.assertExchange(exchange.name, exchange.type, exchange.options);
  }
}

// Caso de uso 1 y 2: notificaciones y tickets (colas de trabajo).
async function declareWorkQueues(ch) {
  for (const queue of [QUEUES.NOTIFICACIONES, QUEUES.TICKETS]) {
    await ch.assertQueue(queue.name, queue.options);

    for (const routingKey of queue.bindings) {
      await ch.bindQueue(queue.name, EXCHANGES.EVENTS.name, routingKey);
    }

    // Retorno desde las colas de reintento: el exchange headers entrega el
    // mensaje solo a la cola indicada en el header pedidos-original-queue.
    await ch.bindQueue(queue.name, EXCHANGES.RETRY_RETURN.name, "", {
      "x-match": "all",
      [HEADERS.ORIGINAL_QUEUE]: queue.name,
    });
  }
}

// Caso de uso 3: reintentos con backoff (una cola de espera por nivel).
async function declareRetryQueues(ch) {
  for (const queue of RETRY_QUEUES) {
    await ch.assertQueue(queue.name, queue.options);
    await ch.bindQueue(queue.name, EXCHANGES.RETRY.name, queue.routingKey);
  }
}

// Caso de uso 4: mensajes muertos (DLX fanout -> DLQ).
async function declareDeadLetter(ch) {
  await ch.assertQueue(QUEUES.DLQ.name, QUEUES.DLQ.options);
  await ch.bindQueue(QUEUES.DLQ.name, EXCHANGES.DLX.name, "");
}

export async function declareTopology(ch) {
  await declareExchanges(ch);
  await declareDeadLetter(ch);
  await declareRetryQueues(ch);
  await declareWorkQueues(ch);
  console.log("[RABBITMQ] Topología declarada:");
  console.log(`  ${EXCHANGES.EVENTS.name} -> ${QUEUES.NOTIFICACIONES.name} [${QUEUES.NOTIFICACIONES.bindings.join(", ")}]`);
  console.log(`  ${EXCHANGES.EVENTS.name} -> ${QUEUES.TICKETS.name} [${QUEUES.TICKETS.bindings.join(", ")}]`);
  console.log(`  ${EXCHANGES.RETRY.name} -> ${RETRY_QUEUES.map((q) => q.name).join(", ")}`);
  console.log(`  ${EXCHANGES.DLX.name} -> ${QUEUES.DLQ.name}`);
}
