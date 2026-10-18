import { EXCHANGES, HEADERS, RETRY_POLICY } from "../config/messaging.js";
import { NonRetryableError } from "./errors.js";

// Equivalente a @RabbitListener con acknowledge-mode: manual. Cada consumidor
// de dominio solo implementa su handler (lógica de negocio); este módulo
// decide qué pasa con el mensaje según el resultado:
//
//   1. Éxito                              -> ACK
//   2. Error recuperable, quedan intentos -> se publica en la cola de espera
//      (backoff 1s/2s/4s) con x-retry-count+1 y luego ACK del original
//   3. Error recuperable, sin intentos    -> NACK(requeue=false): RabbitMQ lo
//      envía al DLX por la configuración de la cola (x-dead-letter-exchange +
//      x-dead-letter-routing-key) y termina en la DLQ de ESA cola
//   4. Error no recuperable               -> se publica directo en el DLX con
//      la routing key de la DLQ de la cola y el motivo, y luego ACK
//
// Nunca se usa requeue=true: evita bucles infinitos de reprocesamiento.

function publishConfirmed(channel, exchange, routingKey, msg, extraHeaders) {
  return new Promise((resolve, reject) => {
    channel.publish(
      exchange,
      routingKey,
      msg.content,
      {
        ...msg.properties,
        persistent: true,
        headers: { ...msg.properties.headers, ...extraHeaders },
      },
      (error) => (error ? reject(error) : resolve()),
    );
  });
}

function parseEvent(msg) {
  try {
    return JSON.parse(msg.content.toString());
  } catch {
    throw new NonRetryableError("El mensaje no es JSON válido");
  }
}

// queue: entrada de QUEUES en config/messaging.js ({ name, dlq: { routingKey } }).
export async function registerListener(channel, queue, handler) {
  const queueName = queue.name;
  await channel.consume(
    queueName,
    async (msg) => {
      if (!msg) return; // el broker canceló el consumidor

      const headers = msg.properties.headers ?? {};
      const retryCount = Number(headers[HEADERS.RETRY_COUNT] ?? 0);
      const tag = `[${queueName}] ${msg.properties.messageId ?? "sin-id"}`;

      try {
        const event = parseEvent(msg);
        await handler(event, msg);
        channel.ack(msg); // (1)
        console.log(`${tag} ✓ ACK (intento ${retryCount + 1})`);
      } catch (error) {
        try {
          if (error instanceof NonRetryableError) {
            // (4)
            await publishConfirmed(channel, EXCHANGES.DLX.name, queue.dlq.routingKey, msg, {
              [HEADERS.ORIGINAL_QUEUE]: queueName,
              [HEADERS.DEAD_REASON]: "no-recuperable",
              [HEADERS.LAST_ERROR]: error.message,
            });
            channel.ack(msg);
            console.error(`${tag} ✗ NO RECUPERABLE -> ${queue.dlq.name}: ${error.message}`);
          } else if (retryCount < RETRY_POLICY.maxRetries) {
            // (2)
            const delayMs = RETRY_POLICY.delaysMs[retryCount];
            await publishConfirmed(channel, EXCHANGES.RETRY.name, String(delayMs), msg, {
              [HEADERS.RETRY_COUNT]: retryCount + 1,
              [HEADERS.ORIGINAL_QUEUE]: queueName,
              [HEADERS.LAST_ERROR]: error.message,
            });
            channel.ack(msg);
            console.warn(
              `${tag} ↻ REINTENTO ${retryCount + 1}/${RETRY_POLICY.maxRetries} en ${delayMs} ms: ${error.message}`,
            );
          } else {
            // (3)
            channel.nack(msg, false, false);
            console.error(`${tag} ✗ NACK tras ${retryCount} reintentos -> ${queue.dlq.name}: ${error.message}`);
          }
        } catch (routingError) {
          // No se pudo republicar (broker caído): NACK sin requeue manda el
          // mensaje a la DLQ por la configuración de la cola; no se pierde.
          console.error(`${tag} No se pudo enrutar el fallo (${routingError.message}). NACK -> ${queue.dlq.name}`);
          try {
            channel.nack(msg, false, false);
          } catch {
            // canal cerrado: RabbitMQ re-entregará el mensaje al reconectar
          }
        }
      }
    },
    { noAck: false },
  );
  console.log(`[CONSUMIDOR] Escuchando '${queueName}'`);
}
