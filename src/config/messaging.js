// Configuración centralizada de mensajería del worker (equivalente a
// application.yml + @Configuration en Spring). TODOS los nombres de exchanges,
// colas, routing keys, headers y políticas viven aquí; el resto del código
// solo importa estas constantes.
//
// Rutas de mensajería:
//
//   pedidos.events (topic) ──pedido.creado──────────▶ pedidos.notificaciones.queue
//                          ──pedido.estado.*────────▶ pedidos.notificaciones.queue
//                          ──usuario.creado─────────▶ pedidos.notificaciones.queue
//                          ──pedido.creado──────────▶ pedidos.tickets.queue
//                          (sin destino: alternate-exchange ▶ pedidos.dlx)
//
//   Reintento:  cola de trabajo ──(falla recuperable)──▶ pedidos.retry (direct)
//               ──"1000"/"2000"/"4000"──▶ pedidos.retry.{ms}ms (TTL = espera)
//               ──expira──▶ pedidos.retry.return (headers: pedidos-original-queue)
//               ──▶ vuelve SOLO a la cola que falló
//
//   DLQ:        cola de trabajo ──nack / TTL / maxlen──▶ pedidos.dlx (fanout)
//               ──▶ pedidos.dlq (retención 24 h) ──▶ consumidor DLQ (log + Supabase)

const DLX_NAME = process.env.RABBITMQ_DLX_EXCHANGE ?? "pedidos.dlx";

export const EXCHANGES = {
  EVENTS: {
    name: process.env.RABBITMQ_EVENTS_EXCHANGE ?? "pedidos.events",
    type: "topic",
    options: { durable: true, alternateExchange: DLX_NAME },
  },
  DLX: {
    name: DLX_NAME,
    type: "fanout",
    options: { durable: true },
  },
  RETRY: {
    name: "pedidos.retry",
    type: "direct",
    options: { durable: true },
  },
  RETRY_RETURN: {
    name: "pedidos.retry.return",
    type: "headers",
    options: { durable: true },
  },
};

export const ROUTING_KEYS = {
  PEDIDO_CREADO: "pedido.creado",
  PEDIDO_ESTADO_ANY: "pedido.estado.*",
  USUARIO_CREADO: "usuario.creado",
};

export const HEADERS = {
  RETRY_COUNT: "x-retry-count",
  // Sin prefijo "x-": el exchange headers IGNORA los headers que empiezan con
  // "x-" al evaluar bindings, y este es justamente el que decide el retorno.
  ORIGINAL_QUEUE: "pedidos-original-queue",
  LAST_ERROR: "x-last-error",
  DEAD_REASON: "x-dead-reason",
  SIMULAR_FALLO: "x-simular-fallo",
};

// Política de reintentos: 3 reintentos con backoff exponencial 1s, 2s, 4s.
export const RETRY_POLICY = {
  delaysMs: [1000, 2000, 4000],
  get maxRetries() {
    return this.delaysMs.length;
  },
};

// Fair dispatch: cada consumidor procesa 1 mensaje a la vez.
export const CONSUMER = {
  prefetch: 1,
};

// Argumentos comunes de las colas de trabajo: si un mensaje se rechaza,
// expira (TTL) o la cola se llena (max-length), RabbitMQ lo manda al DLX.
const workQueueArguments = {
  "x-dead-letter-exchange": DLX_NAME,
  "x-message-ttl": Number(process.env.WORK_QUEUE_TTL_MS) || 600000, // 10 min
  "x-max-length": 1000,
};

export const QUEUES = {
  NOTIFICACIONES: {
    name: "pedidos.notificaciones.queue",
    options: { durable: true, arguments: workQueueArguments },
    bindings: [ROUTING_KEYS.PEDIDO_CREADO, ROUTING_KEYS.PEDIDO_ESTADO_ANY, ROUTING_KEYS.USUARIO_CREADO],
  },
  TICKETS: {
    name: "pedidos.tickets.queue",
    options: { durable: true, arguments: workQueueArguments },
    bindings: [ROUTING_KEYS.PEDIDO_CREADO],
  },
  DLQ: {
    name: "pedidos.dlq",
    // Retención: los mensajes muertos se conservan 24 h para auditoría.
    options: { durable: true, arguments: { "x-message-ttl": 86400000 } },
  },
};

// Una cola de espera por cada nivel de backoff. No tienen consumidores: el
// mensaje espera su TTL y al expirar vuelve por pedidos.retry.return.
export const RETRY_QUEUES = RETRY_POLICY.delaysMs.map((delayMs) => ({
  name: `pedidos.retry.${delayMs}ms`,
  routingKey: String(delayMs),
  options: {
    durable: true,
    arguments: {
      "x-message-ttl": delayMs,
      "x-dead-letter-exchange": EXCHANGES.RETRY_RETURN.name,
    },
  },
}));

export const CONNECTION = {
  maxRetries: 10,
  retryDelayMs: 3000,
};
