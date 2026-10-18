import amqp from "amqplib";
import { env } from "../config/env.js";
import { CONNECTION, CONSUMER } from "../config/messaging.js";
import { declareTopology } from "./topology.js";

// Conexión única a RabbitMQ. ConfirmChannel: cuando el listener republica un
// mensaje (a reintento o a la DLX) espera la confirmación del broker ANTES de
// hacer ack del original, así nunca se pierde un mensaje en el camino.
//
// Nunca se rinde: si el broker no responde, se cae la conexión o se cierra
// el canal, reintenta con espera creciente (3s, 6s, 12s... máx. 30s) y al
// reconectar vuelve a declarar la topología y a registrar los consumidores.
let connection = null;
let channel = null;
let closing = false;
let connecting = false;
let onReadyHandler = null;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// RABBITMQ_URL puede traer varios nodos del clúster separados por coma
// (amqp://u:p@host:5672,amqp://u:p@host:5673): si uno no responde se prueba
// el siguiente, así el servicio sigue funcionando si se cae un nodo.
const urls = env.rabbitmqUrl.split(",").map((url) => url.trim()).filter(Boolean);
const safeHost = (url) => url.replace(/\/\/[^@]*@/, "//***@");

// Heartbeat: detecta conexiones muertas (red caída, broker reiniciado) en
// segundos, en vez de creer que siguen vivas.
const withHeartbeat = (url) =>
  url.includes("heartbeat=") ? url : `${url}${url.includes("?") ? "&" : "?"}heartbeat=${CONNECTION.heartbeatSeconds}`;

const retryDelay = (attempt) =>
  Math.min(CONNECTION.retryDelayMs * 2 ** Math.min(attempt - 1, 4), CONNECTION.maxRetryDelayMs);

async function openConnection() {
  for (let attempt = 1; ; attempt++) {
    const url = urls[(attempt - 1) % urls.length];
    try {
      const conn = await amqp.connect(withHeartbeat(url));
      console.log(`[RABBITMQ] Conectado a ${safeHost(url)}`);
      return conn;
    } catch (error) {
      const delay = retryDelay(attempt);
      console.warn(
        `[RABBITMQ] Intento ${attempt} a ${safeHost(url)} fallido (${error.message || error.code || "sin respuesta"}). Reintento en ${delay / 1000}s`,
      );
      await sleep(delay);
    }
  }
}

function scheduleReconnect() {
  if (closing) return;
  setTimeout(() => void connectRabbit(onReadyHandler), CONNECTION.retryDelayMs);
}

// onReady se vuelve a ejecutar tras cada reconexión para re-registrar los
// consumidores en el canal nuevo.
export async function connectRabbit(onReady) {
  onReadyHandler = onReady;
  if (connecting || closing) return;
  connecting = true;

  try {
    const conn = await openConnection();
    connection = conn;
    conn.on("error", (error) => console.error("[RABBITMQ] Error de conexión:", error.message));
    conn.on("close", () => {
      if (conn !== connection) return; // conexión vieja, ya reemplazada
      channel = null;
      connection = null;
      console.warn("[RABBITMQ] Conexión cerrada. Reconectando...");
      scheduleReconnect();
    });

    const ch = await conn.createConfirmChannel();
    ch.on("error", (error) => console.error("[RABBITMQ] Error de canal:", error.message));
    ch.on("close", () => {
      if (ch !== channel || closing) return;
      // El broker cerró solo el canal: se reinicia la conexión completa
      // (al reconectar se re-registran los consumidores).
      channel = null;
      console.warn("[RABBITMQ] Canal cerrado por el broker. Reiniciando conexión...");
      conn.close().catch(() => {});
    });

    await ch.prefetch(CONSUMER.prefetch);
    await declareTopology(ch);
    channel = ch;
    await onReady(ch);
  } catch (error) {
    console.error("[RABBITMQ] No se pudo preparar el canal/consumidores:", error.message);
    const conn = connection;
    connection = null;
    channel = null;
    if (conn) await conn.close().catch(() => {});
    scheduleReconnect();
  } finally {
    connecting = false;
  }
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
