// Configuración centralizada de Kafka del worker (equivalente a
// config/messaging.js, pero para el transporte de streaming). TODOS los
// nombres de tópicos, consumer groups, headers y políticas viven aquí; el
// resto del código solo importa estas constantes.
//
// Rutas de streaming (3 consumer groups, cada uno con su responsabilidad):
//
//   pedidos.eventos.v1 ──grupo pedidos360-analitica──▶ agrega ventas por
//      (3 particiones)                                  producto en Supabase
//                                                             │
//                                                             ▼ produce
//   pedidos.metricas.v1 ──grupo pedidos360-proyeccion──▶ snapshot para el front
//      (3 particiones, COMPACTADO: último valor por productoId)
//
//   pedidos.eventos.dlt.v1 ──grupo pedidos360-dlt-monitor──▶ tabla mensajes_dlq
//      (1 partición, retención 30 días)
//
// El back-principal consume pedidos.eventos.v1 con el grupo
// pedidos360-back-cache: el MISMO tópico, otro grupo. Cada grupo recibe todos
// los eventos y lleva su propio offset, en vez de competir por el mensaje.
// Dentro de un grupo, en cambio, las 3 particiones se reparten entre sus
// miembros: levantar un segundo worker duplica la capacidad de proceso.
//
// Manejo de errores (política del kafkaListener):
//
//   éxito                              -> commit del offset
//   error recuperable, quedan intentos -> espera 1s, 2s, 4s y reintenta
//   error recuperable, sin intentos    -> al DLT + commit
//   error NO recuperable (payload      -> al DLT directo, sin reintentos,
//   inválido, JSON roto)                  + commit

export const TOPICS = {
  EVENTOS: process.env.KAFKA_EVENTOS_TOPIC ?? "pedidos.eventos.v1",
  METRICAS: process.env.KAFKA_METRICAS_TOPIC ?? "pedidos.metricas.v1",
  DLT: process.env.KAFKA_DLT_TOPIC ?? "pedidos.eventos.dlt.v1",
};

export const CONSUMER_GROUPS = {
  ANALITICA: process.env.KAFKA_ANALITICA_GROUP_ID ?? "pedidos360-analitica",
  PROYECCION: process.env.KAFKA_PROYECCION_GROUP_ID ?? "pedidos360-proyeccion",
  DLT_MONITOR: process.env.KAFKA_DLT_GROUP_ID ?? "pedidos360-dlt-monitor",
};

export const HEADERS = {
  EVENTO_ID: "evento-id",
  TIPO: "tipo",
  ORIGEN: "origen",
  // Lo manda el panel del Admin para forzar un fallo y mostrar en vivo los
  // reintentos y el DLT (equivalente a x-simular-fallo en RabbitMQ).
  SIMULAR_FALLO: "simular-fallo",
  // Los que agrega el listener al mandar un mensaje al DLT:
  INTENTOS: "intentos",
  MOTIVO: "motivo",
  ULTIMO_ERROR: "ultimo-error",
  TOPIC_ORIGEN: "topic-origen",
  PARTICION_ORIGEN: "particion-origen",
  OFFSET_ORIGEN: "offset-origen",
  GRUPO_ORIGEN: "grupo-origen",
};

export const ORIGEN = "pedidos360-worker";

// Motivos por los que un mensaje termina en el Dead Letter Topic. Son los
// mismos conceptos que ya usa la DLQ de RabbitMQ, para que el panel muestre
// ambos orígenes con el mismo vocabulario.
export const MOTIVOS_DLT = {
  SIN_REINTENTOS: "rejected",
  NO_RECUPERABLE: "no-recuperable",
};

// Misma política de reintentos que RabbitMQ (config/messaging.js): 3 intentos
// con backoff exponencial 1s, 2s, 4s.
export const RETRY_POLICY = {
  delaysMs: [1000, 2000, 4000],
  get maxRetries() {
    return this.delaysMs.length;
  },
};

export const CONSUMER = {
  // Una partición a la vez por consumidor: el orden dentro de cada partición
  // se respeta y el código queda simple de seguir (equivale al prefetch 1 de
  // RabbitMQ). Para escalar se levantan más workers, no más concurrencia.
  partitionsConsumedConcurrently: 1,
};

// Mismos tiempos de reconexión que RabbitMQ: 3s, 6s, 12s, 24s y luego 30s.
export const CONNECTION = {
  retryDelayMs: 3000,
  maxRetryDelayMs: 30000,
};
