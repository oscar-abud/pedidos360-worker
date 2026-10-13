import express from "express";
import { env } from "./config/env.js";
import { closeRabbit, connectRabbit, isRabbitConnected } from "./messaging/rabbitConnection.js";
import { registerConsumers } from "./consumers/index.js";

// El worker no expone API de negocio: solo un health check HTTP (Render lo
// necesita para un Web Service y sirve para saber si está consumiendo).
const app = express();

app.get("/health", (req, res) => {
  const connected = isRabbitConnected();
  res.status(connected ? 200 : 503).json({
    status: connected ? "ok" : "degradado",
    rabbitmq: connected ? "conectado" : "desconectado",
  });
});

app.listen(env.port, () => {
  console.log(`[WORKER] Health check en http://localhost:${env.port}/health`);
});

connectRabbit(registerConsumers).catch((error) => {
  console.error("[FATAL] No se pudo conectar con RabbitMQ:", error.message);
  process.exit(1);
});

async function shutdown() {
  console.log("[WORKER] Cerrando conexión con RabbitMQ...");
  await closeRabbit();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
