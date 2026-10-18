// Configuración centralizada de mensajería del worker (equivalente a
// application.yml + @Configuration en Spring). TODOS los nombres de exchanges,
// colas, routing keys, headers y políticas viven aquí; el resto del código
// solo importa estas constantes.
//
// Rutas de mensajería (3 colas de trabajo, cada una con su DLQ):
//
//   pedidos.events (topic) ──pedido.creado / pedido.estado.* / usuario.creado──▶ pedidos.notificaciones.queue
//                          ──pedido.creado──────────────────────────────────────▶ pedidos.tickets.queue
//                          ──# (todos los eventos)──────────────────────────────▶ pedidos.auditoria.queue
//
//   Reintento:  cola de trabajo ──(falla recuperable)──▶ pedidos.retry (direct)
//               ──"1000"/"2000"/"4000"──▶ pedidos.retry.{ms}ms (TTL = espera)
//               ──expira──▶ pedidos.retry.return (headers: pedidos-original-queue)
//               ──▶ vuelve SOLO a la cola que falló
//
//   DLQ:        cola de trabajo ──nack / TTL / maxlen──▶ pedidos.dlx (direct)
//               ──"notificaciones"──▶ pedidos.notificaciones.dlq
//               ──"tickets"─────────▶ pedidos.tickets.dlq
//               ──"auditoria"───────▶ pedidos.auditoria.dlq   ──▶ consumidor DLQ (log + Supabase)
//
// Clúster de 2 nodos: colas de trabajo y DLQs son "quorum" (replicadas en
// ambos nodos). Las colas de espera de reintento son "classic" (efímeras).

export const EXCHANGES = {
  EVENTS: {
    name: process.env.RABBITMQ_EVENTS_EXCHANGE ?? "pedidos.events",
    type: "topic",
    options: { durable: true },
  },
  DLX: {
    name: process.env.RABBITMQ_DLX_EXCHANGE ?? "pedidos.dlx",
    type: "direct",
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
  ALL: "#",
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

const WORK_QUEUE_TTL_MS = Number(process.env.WORK_QUEUE_TTL_MS) || 600000; // 10 min
const DLQ_RETENTION_MS = 86400000; // 24 h

// Cola de trabajo + su DLQ. Si un mensaje se rechaza (NACK), expira (TTL) o
// la cola se llena (max-length), RabbitMQ lo manda al DLX con la routing key
// de la cola, y el DLX lo deja en SU DLQ.
function workQueue(name, dlqKey, bindings) {
  return {
    name,
    bindings,
    options: {
      durable: true,
      arguments: {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": EXCHANGES.DLX.name,
        "x-dead-letter-routing-key": dlqKey,
        "x-message-ttl": WORK_QUEUE_TTL_MS,
        "x-max-length": 1000,
      },
    },
    dlq: {
      name: name.replace(/\.queue$/, ".dlq"),
      routingKey: dlqKey,
      options: {
        durable: true,
        arguments: { "x-queue-type": "quorum", "x-message-ttl": DLQ_RETENTION_MS },
      },
    },
  };
}

export const QUEUES = {
  NOTIFICACIONES: workQueue("pedidos.notificaciones.queue", "notificaciones", [
    ROUTING_KEYS.PEDIDO_CREADO,
    ROUTING_KEYS.PEDIDO_ESTADO_ANY,
    ROUTING_KEYS.USUARIO_CREADO,
  ]),
  TICKETS: workQueue("pedidos.tickets.queue", "tickets", [ROUTING_KEYS.PEDIDO_CREADO]),
  AUDITORIA: workQueue("pedidos.auditoria.queue", "auditoria", [ROUTING_KEYS.ALL]),
};

export const WORK_QUEUES = [QUEUES.NOTIFICACIONES, QUEUES.TICKETS, QUEUES.AUDITORIA];

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

// Reconexión sin límite: espera 3s, 6s, 12s, 24s y luego 30s entre intentos.
export const CONNECTION = {
  retryDelayMs: 3000,
  maxRetryDelayMs: 30000,
  heartbeatSeconds: 30,
};
