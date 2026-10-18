import amqp from "amqplib";
import { env } from "../config/env.js";
import { CONNECTION, CONSUMER } from "../config/messaging.js";
import { declareTopology } from "./topology.js";

// Conexión única a RabbitMQ. ConfirmChannel: cuando el listener republica un
// mensaje (a reintento o a la DLX) espera la confirmación del broker ANTES de
// hacer ack del original, así nunca se pierde un mensaje en el camino.
let connection = null;
let channel = null;
let closing = false;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// RABBITMQ_URL puede traer varios nodos del clúster separados por coma
// (amqp://u:p@host:5672,amqp://u:p@host:5673): si uno no responde se prueba
// el siguiente, así el servicio sigue funcionando si se cae un nodo.
const urls = env.rabbitmqUrl.split(",").map((url) => url.trim()).filter(Boolean);
const safeHost = (url) => url.replace(/\/\/[^@]*@/, "//***@");

async function openConnection() {
  for (let attempt = 1; attempt <= CONNECTION.maxRetries; attempt++) {
    const url = urls[(attempt - 1) % urls.length];
    try {
      const conn = await amqp.connect(url);
      console.log(`[RABBITMQ] Conectado a ${safeHost(url)}`);
      return conn;
    } catch (error) {
      console.warn(`[RABBITMQ] Intento ${attempt}/${CONNECTION.maxRetries} a ${safeHost(url)} fallido (${error.message}).`);
      if (attempt === CONNECTION.maxRetries) throw error;
      await sleep(CONNECTION.retryDelayMs);
    }
  }
}

// onReady se vuelve a ejecutar tras cada reconexión para re-registrar los
// consumidores en el canal nuevo.
export async function connectRabbit(onReady) {
  connection = await openConnection();
  connection.on("error", (error) => console.error("[RABBITMQ] Error de conexión:", error.message));
  connection.on("close", () => {
    channel = null;
    connection = null;
    if (!closing) {
      console.warn("[RABBITMQ] Conexión cerrada. Reconectando...");
      setTimeout(
        () => connectRabbit(onReady).catch((e) => console.error("[RABBITMQ]", e.message)),
        CONNECTION.retryDelayMs,
      );
    }
  });

  channel = await connection.createConfirmChannel();
  await channel.prefetch(CONSUMER.prefetch);
  await declareTopology(channel);
  await onReady(channel);
}

export function isRabbitConnected() {
  return Boolean(channel);
}

export async function closeRabbit() {
  closing = true;
  try {
    if (channel) await channel.close();
    if (connection) await connection.close();
  } catch {
    // ya estaba cerrada
  }
}
