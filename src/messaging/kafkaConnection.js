import { crearProducer } from "../config/kafkaClient.js";
import { env } from "../config/env.js";
import { CONNECTION } from "../config/kafka.js";

// Conexión del worker con Kafka (Singleton, igual que rabbitConnection.js).
//
// El worker es sobre todo consumidor, pero también necesita un productor para
// dos cosas: mandar al Dead Letter Topic lo que falla y publicar las métricas
// que calcula. Por eso el productor se conecta primero y solo después se
// registran los consumidores: si un mensaje falla en el primer segundo, el DLT
// ya tiene que estar disponible.
//
// Nunca se rinde: si el clúster no responde, reintenta con espera creciente
// (3s, 6s, 12s... máx. 30s) y al reconectar vuelve a registrar los consumidores.

let producer = null;
let closing = false;
let connecting = false;
let onReadyHandler = null;

// Los consumidores se guardan para poder cerrarlos ordenadamente en el
// shutdown: así Kafka reparte sus particiones de inmediato en vez de esperar
// a que expire el session timeout.
const consumidores = new Set();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const retryDelay = (attempt) =>
  Math.min(CONNECTION.retryDelayMs * 2 ** Math.min(attempt - 1, 4), CONNECTION.maxRetryDelayMs);

export function registrarConsumidor(consumer) {
  consumidores.add(consumer);
}

export async function connectKafka(onReady) {
  onReadyHandler = onReady;

  if (!env.kafkaEnabled) {
    console.warn("[KAFKA] KAFKA_ENABLED=false: el worker no consumirá de Kafka.");
    return;
  }
  if (!env.kafkaBrokers.length) {
    console.warn("[KAFKA] Sin KAFKA_BROKERS: el worker no consumirá de Kafka.");
    return;
  }
  if (connecting || closing) return;
  connecting = true;

  try {
    for (let attempt = 1; ; attempt++) {
      const instancia = crearProducer();
      try {
        await instancia.connect();
        producer = instancia;
        console.log(
          `[KAFKA] Productor conectado a ${env.kafkaBrokers.join(", ")} (clientId ${env.kafkaClientId})`,
        );

        await onReadyHandler();
        return;
      } catch (error) {
        await instancia.disconnect().catch(() => {});
        producer = null;
        const delay = retryDelay(attempt);
        console.warn(
          `[KAFKA] Intento ${attempt} fallido (${error.message || error.code || "sin respuesta"}). Reintento en ${delay / 1000}s`,
        );
        await sleep(delay);
      }
    }
  } finally {
    connecting = false;
  }
}

export function getProducer() {
  return producer;
}

export function isKafkaConnected() {
  return Boolean(producer);
}

export async function closeKafka() {
  closing = true;

  // Primero los consumidores (dejan de recibir mensajes) y después el
  // productor, que todavía puede necesitarse para un último envío al DLT.
  for (const consumer of consumidores) {
    await consumer.disconnect().catch(() => {});
  }
  consumidores.clear();

  try {
    if (producer) await producer.disconnect();
  } catch {
    // ya estaba cerrado
  }
  producer = null;
}
