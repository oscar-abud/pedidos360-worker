// KafkaJS se publica como CommonJS: en ESM hay que importar el default y
// desestructurar, porque Node no expone todos sus named exports.
import kafkajs from "kafkajs";
import { env } from "./env.js";

const { Kafka, logLevel, Partitioners } = kafkajs;

// Configuración centralizada del cliente Kafka (equivalente a las propiedades
// spring.kafka.* de application.yml). Nadie más instancia un Kafka: todos
// importan de acá, igual que con supabaseClient.js.

export const kafka = new Kafka({
  // Identifica a este servicio en los logs del broker y en Kafka-UI.
  clientId: env.kafkaClientId,
  brokers: env.kafkaBrokers,

  connectionTimeout: 10000,
  requestTimeout: 30000,

  // Reintentos ante errores transitorios (broker reiniciando, rebalanceo, red
  // intermitente): 300ms, 600ms, 1.2s... hasta 30s, 8 veces.
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 30000,
    factor: 2,
  },

  // WARN y no INFO: KafkaJS en INFO es muy verboso y tapa los logs propios.
  logLevel: logLevel.WARN,
});

export function crearProducer() {
  return kafka.producer({
    // Particionador por hash de la key (el estándar de Kafka): la misma key
    // cae siempre en la misma partición. El legacy de KafkaJS usaba otro hash
    // incompatible con el resto del ecosistema.
    createPartitioner: Partitioners.DefaultPartitioner,

    // Idempotencia: si un envío se reintenta por un corte de red, el broker
    // detecta el duplicado y lo descarta. Implica acks=all automáticamente,
    // que junto a min.insync.replicas=2 significa que una escritura confirmada
    // sobrevive a la caída de un broker.
    idempotent: true,
    maxInFlightRequests: 5,

    // KafkaJS avisa al arrancar que "limitar los reintentos del productor
    // idempotente puede invalidar las garantías EoS": la idempotencia estricta
    // pediría reintentos infinitos. Se limitan a propósito, porque el listener
    // NO hace commit si no logra escribir en el DLT: el mensaje se vuelve a
    // entregar más adelante en vez de quedarse colgado reintentando para siempre.

    // Los tópicos los crea el administrador con sus particiones y políticas;
    // si un productor pudiera autocrearlos, nacerían con los valores por defecto.
    allowAutoTopicCreation: false,
  });
}

export function crearConsumer(groupId) {
  return kafka.consumer({
    // El groupId define el grupo: Kafka reparte las particiones entre los
    // miembros del mismo grupo y entrega una copia a cada grupo distinto.
    groupId,

    // Si el consumidor no manda heartbeat en 30s, Kafka lo da por caído y
    // reparte sus particiones entre el resto del grupo.
    sessionTimeout: 30000,
    heartbeatInterval: 3000,

    maxBytesPerPartition: 1048576, // 1 MB
    allowAutoTopicCreation: false,
  });
}
