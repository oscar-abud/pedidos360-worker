import { crearConsumer } from "../config/kafkaClient.js";
import { registrarConsumidor } from "./kafkaConnection.js";
import { CONSUMER, MOTIVOS_DLT, RETRY_POLICY, TOPICS } from "../config/kafka.js";
import { NonRetryableError } from "./errors.js";
import { parseEvento } from "../events/eventSchemas.js";
import * as KafkaProducer from "./kafkaProducer.js";

// Equivalente a @KafkaListener de Spring y hermano de listener.js (RabbitMQ).
// Cada consumidor de dominio solo implementa su handler (lógica de negocio);
// este módulo decide qué pasa con el mensaje según el resultado:
//
//   1. Éxito                              -> commit del offset
//   2. Error recuperable, quedan intentos -> espera con backoff (1s, 2s, 4s)
//                                            y reintenta el MISMO mensaje
//   3. Error recuperable, sin intentos    -> al Dead Letter Topic + commit
//   4. Error no recuperable (JSON roto o  -> al Dead Letter Topic directo,
//      payload que no pasa la validación)    sin reintentos, + commit
//
// Nunca se hace commit antes de procesar: si el worker se cae a mitad de
// camino, Kafka vuelve a entregar el mensaje en vez de darlo por hecho.
//
// Si NO se logra escribir en el DLT tampoco se hace commit: el mensaje se
// volverá a entregar más adelante. Es preferible procesar dos veces (los
// servicios son idempotentes) a perder el evento.

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Durante la espera del backoff hay que seguir mandando heartbeats, o Kafka
// cree que el consumidor murió y dispara un rebalanceo en plena espera.
async function esperarConHeartbeat(ms, heartbeat) {
  const fin = Date.now() + ms;
  while (Date.now() < fin) {
    await sleep(Math.min(1000, fin - Date.now()));
    await heartbeat();
  }
}

function deserializar(message, validar) {
  let crudo;
  try {
    crudo = JSON.parse(message.value.toString());
  } catch {
    throw new NonRetryableError("El mensaje no es JSON válido");
  }

  // El consumidor del DLT recibe mensajes que ya se sabe que vienen rotos:
  // ahí no se valida, se registran tal cual para poder diagnosticarlos.
  if (!validar) return crudo;

  const resultado = parseEvento(crudo);
  if (!resultado.exito) {
    throw new NonRetryableError(
      `El evento no cumple el contrato: ${resultado.errores.join("; ")}`,
    );
  }
  return resultado.evento;
}

/**
 * Registra un consumidor sobre un tópico.
 *
 * @param opciones.groupId    consumer group (define el reparto de particiones)
 * @param opciones.topic      tópico a consumir
 * @param opciones.fromBeginning  true para leer el histórico retenido
 * @param opciones.validar    aplicar el contrato de eventos (zod)
 * @param opciones.enviarADlt false para el consumidor del propio DLT
 * @param handler             async (payload, contexto) => void
 */
export async function registerKafkaListener(opciones, handler) {
  const {
    groupId,
    topic,
    fromBeginning = true,
    validar = true,
    enviarADlt = true,
  } = opciones;

  const consumer = crearConsumer(groupId);
  registrarConsumidor(consumer);

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning });

  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: CONSUMER.partitionsConsumedConcurrently,

    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const contexto = {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        groupId,
        // Kafka entrega los headers como Buffers: se pasan ya decodificados
        // para que los consumidores no tengan que hacerlo cada vez.
        headers: Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([clave, valor]) => [
            clave,
            valor?.toString() ?? null,
          ]),
        ),
      };
      // Todo log lleva el contexto completo: grupo, tópico, partición y offset.
      // Con eso se localiza cualquier mensaje en Kafka-UI.
      const tag = `[${groupId}] ${topic}[${partition}]@${message.offset}`;

      const commit = () =>
        consumer.commitOffsets([
          // El offset que se confirma es el SIGUIENTE a leer, por eso el +1.
          { topic, partition, offset: (Number(message.offset) + 1).toString() },
        ]);

      let intentos = 0;
      let ultimoError = null;
      let motivo = MOTIVOS_DLT.SIN_REINTENTOS;

      while (true) {
        try {
          const payload = deserializar(message, validar);
          await handler(payload, contexto);
          await commit();
          console.log(`${tag} ✓ COMMIT (intento ${intentos + 1}) key=${contexto.key}`);
          return;
        } catch (error) {
          ultimoError = error.message;

          if (error instanceof NonRetryableError) {
            motivo = MOTIVOS_DLT.NO_RECUPERABLE;
            break;
          }

          if (intentos < RETRY_POLICY.maxRetries) {
            const delayMs = RETRY_POLICY.delaysMs[intentos];
            intentos += 1;
            console.warn(
              `${tag} ↻ REINTENTO ${intentos}/${RETRY_POLICY.maxRetries} en ${delayMs} ms: ${error.message}`,
            );
            await esperarConHeartbeat(delayMs, heartbeat);
            continue;
          }

          // Se agotaron los reintentos: cualquier otro error inesperado se
          // trata como recuperable (conservador: mejor reintentar que perder).
          motivo = MOTIVOS_DLT.SIN_REINTENTOS;
          break;
        }
      }

      if (!enviarADlt) {
        // Consumidor del propio DLT: no hay un "DLT del DLT". El mensaje sigue
        // retenido 30 días en el tópico, así que se puede reprocesar con
        // reset-offsets desde el administrador.
        await commit();
        console.error(`${tag} ✗ ${motivo} (sin DLT, queda retenido): ${ultimoError}`);
        return;
      }

      try {
        const metadata = await KafkaProducer.publicarEnDlt({
          message,
          contexto,
          motivo,
          intentos,
          error: ultimoError,
        });
        await commit();
        console.error(
          `${tag} ☠ ${motivo} -> ${TOPICS.DLT}[${metadata.partition}]@${metadata.baseOffset} tras ${intentos} reintentos: ${ultimoError}`,
        );
      } catch (errorDlt) {
        // Sin commit: Kafka volverá a entregar el mensaje. No se pierde.
        console.error(
          `${tag} No se pudo escribir en el DLT (${errorDlt.message}). Sin commit: el mensaje se volverá a entregar`,
        );
      }
    },
  });

  console.log(`[CONSUMIDOR] '${groupId}' escuchando '${topic}'`);
  return consumer;
}
