import { HEADERS, WORK_QUEUES } from "../config/messaging.js";
import * as DlqService from "../services/dlqService.js";

// Dominio: FALLOS. Escucha las 3 DLQ (una por cola de trabajo), registra en
// el log por qué murió cada mensaje y lo guarda en Supabase (panel DLQ).
// A diferencia de los otros consumidores SÍ hace ACK al terminar (la guía
// 2.2.3 no lo hacía y el consumidor quedaba trabado tras el primer mensaje).

const REINTENTO_PERSISTENCIA_MS = 5000;

// Motivo según cómo llegó el mensaje al DLX:
//  - x-death (lo agrega RabbitMQ): rejected (NACK), expired (TTL), maxlen
//  - x-dead-reason (lo agrega el listener): no-recuperable
function describirMuerte(msg, colaOrigen) {
  const headers = msg.properties.headers ?? {};
  const death = Array.isArray(headers["x-death"]) ? headers["x-death"][0] : null;

  return {
    motivo: headers[HEADERS.DEAD_REASON] ?? death?.reason ?? "desconocido",
    colaOrigen: headers[HEADERS.ORIGINAL_QUEUE] ?? death?.queue ?? colaOrigen,
    intentos: Number(headers[HEADERS.RETRY_COUNT] ?? 0),
    error: headers[HEADERS.LAST_ERROR] ?? null,
  };
}

function leerPayload(msg) {
  try {
    return JSON.parse(msg.content.toString());
  } catch {
    return { raw: msg.content.toString() };
  }
}

function consumeDlq(channel, queue) {
  return channel
    .consume(
      queue.dlq.name,
      async (msg) => {
        if (!msg) return;

        const payload = leerPayload(msg);
        const muerte = describirMuerte(msg, queue.name);
        const tipo = msg.properties.type ?? payload.tipo ?? msg.fields.routingKey;

        console.error(
          `[DLQ ${queue.dlq.name}] ☠ Mensaje en cuarentena | evento=${msg.properties.messageId ?? "?"} tipo=${tipo} ` +
            `cola=${muerte.colaOrigen} motivo=${muerte.motivo} intentos=${muerte.intentos} ` +
            `error=${muerte.error ?? "-"}`,
        );

        try {
          await DlqService.registrar({
            eventoId: msg.properties.messageId ?? payload.id ?? null,
            tipo,
            ...muerte,
            payload,
          });
          channel.ack(msg);
        } catch (error) {
          // Supabase caído: el mensaje se devuelve a la DLQ (no se pierde) y
          // se reintenta en unos segundos.
          console.error(`[DLQ] ${error.message}. Se reintentará en ${REINTENTO_PERSISTENCIA_MS / 1000}s`);
          setTimeout(() => {
            try {
              channel.nack(msg, false, true);
            } catch {
              // canal cerrado: RabbitMQ re-entrega al reconectar
            }
          }, REINTENTO_PERSISTENCIA_MS);
        }
      },
      { noAck: false },
    )
    .then(() => console.log(`[CONSUMIDOR] Escuchando '${queue.dlq.name}'`));
}

export async function registerDlqConsumers(channel) {
  for (const queue of WORK_QUEUES) {
    await consumeDlq(channel, queue);
  }
}
