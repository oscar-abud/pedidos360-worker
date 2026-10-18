import { EXCHANGES, HEADERS, RETRY_QUEUES, WORK_QUEUES } from "../config/messaging.js";

// Declaración de exchanges, colas y bindings (equivalente a los @Bean Queue,
// Exchange y Binding de Spring). Todas las operaciones son idempotentes: si
// ya existen con los mismos argumentos, RabbitMQ no hace nada.

async function declareExchanges(ch) {
  for (const exchange of Object.values(EXCHANGES)) {
    await ch.assertExchange(exchange.name, exchange.type, exchange.options);
  }
}

// Casos de uso 1-3: notificaciones, tickets y auditoría. Cada cola de trabajo
// se declara junto a su DLQ propia.
async function declareWorkQueues(ch) {
  for (const queue of WORK_QUEUES) {
    // DLQ de esta cola: pedidos.dlx (direct) --routingKey--> DLQ
    await ch.assertQueue(queue.dlq.name, queue.dlq.options);
    await ch.bindQueue(queue.dlq.name, EXCHANGES.DLX.name, queue.dlq.routingKey);

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

// Caso de uso 4: reintentos con backoff (una cola de espera por nivel).
async function declareRetryQueues(ch) {
  for (const queue of RETRY_QUEUES) {
    await ch.assertQueue(queue.name, queue.options);
    await ch.bindQueue(queue.name, EXCHANGES.RETRY.name, queue.routingKey);
  }
}

export async function declareTopology(ch) {
  await declareExchanges(ch);
  await declareRetryQueues(ch);
  await declareWorkQueues(ch);
  console.log("[RABBITMQ] Topología declarada:");
  for (const queue of WORK_QUEUES) {
    console.log(
      `  ${EXCHANGES.EVENTS.name} [${queue.bindings.join(", ")}] -> ${queue.name}` +
        `  | DLQ: ${EXCHANGES.DLX.name} [${queue.dlq.routingKey}] -> ${queue.dlq.name}`,
    );
  }
  console.log(`  ${EXCHANGES.RETRY.name} -> ${RETRY_QUEUES.map((q) => q.name).join(", ")}`);
}
