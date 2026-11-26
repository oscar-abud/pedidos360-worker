import express from "express";
import { env } from "./config/env.js";
import { closeRabbit, connectRabbit, isRabbitConnected } from "./messaging/rabbitConnection.js";
import { closeKafka, connectKafka, isKafkaConnected } from "./messaging/kafkaConnection.js";
import { registerConsumers } from "./consumers/index.js";
import { registerKafkaConsumers } from "./consumers/kafka/index.js";
import * as ProyeccionService from "./services/proyeccionService.js";

// El worker no expone API de negocio: solo un health check HTTP (Render lo
// necesita para un Web Service y sirve para saber si está consumiendo) y el
// ranking que proyecta desde el tópico compactado de métricas.
const app = express();

app.get("/health", (req, res) => {
  const connected = isRabbitConnected();
  res.status(connected ? 200 : 503).json({
    status: connected ? "ok" : "degradado",
    rabbitmq: connected ? "conectado" : "desconectado",
    // Kafka es opcional (KAFKA_ENABLED): su estado se informa, pero no
    // determina la salud del servicio. Si Kafka se cae, el worker sigue
    // procesando los pedidos por RabbitMQ exactamente igual que antes.
    kafka: !env.kafkaEnabled
      ? "deshabilitado"
      : isKafkaConnected()
        ? "conectado"
        : "desconectado",
  });
});

// Ranking de productos más vendidos, reconstruido en memoria desde el tópico
// compactado pedidos.metricas.v1 por el grupo pedidos360-proyeccion.
app.get("/metricas", (req, res) => {
  res.json(ProyeccionService.getRanking());
});

app.listen(env.port, () => {
  console.log(`[WORKER] Health check en http://localhost:${env.port}/health`);
});

connectRabbit(registerConsumers).catch((error) => {
  console.error("[FATAL] No se pudo conectar con RabbitMQ:", error.message);
  process.exit(1);
});

// Kafka NO es fatal: si no se puede conectar, el worker sigue consumiendo de
// RabbitMQ con normalidad. Así la EP5 no puede romper lo que ya funcionaba.
connectKafka(registerKafkaConsumers).catch((error) => {
  console.error("[KAFKA] No se pudieron registrar los consumidores:", error.message);
});

async function shutdown() {
  console.log("[WORKER] Cerrando conexiones con RabbitMQ y Kafka...");
  await closeKafka();
  await closeRabbit();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
