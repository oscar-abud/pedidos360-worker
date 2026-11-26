import { getProducer } from "./kafkaConnection.js";
import { HEADERS, ORIGEN, TOPICS } from "../config/kafka.js";

// Productor del worker. Publica dos cosas:
//   - las métricas que calcula el consumidor de analítica
//   - los mensajes que fallaron, al Dead Letter Topic
//
// A diferencia del productor del back, aquí los envíos SÍ importan: si no se
// puede escribir en el DLT, el listener no hace commit y el mensaje se vuelve
// a entregar. Por eso estas funciones lanzan en vez de tragarse el error.

const serializar = (valor) => Buffer.from(JSON.stringify(valor), "utf8");

// Publica el acumulado de un producto en el tópico compactado de métricas.
// La key es el productoId: Kafka conserva solo el último mensaje de cada key,
// así el tópico funciona como una tabla de estado que no crece sin control.
export async function publicarMetrica(metrica) {
  const producer = getProducer();
  if (!producer) throw new Error("El productor de Kafka no está conectado");

  const [metadata] = await producer.send({
    topic: TOPICS.METRICAS,
    compression: 1, // GZIP
    messages: [
      {
        key: metrica.productoId,
        value: serializar(metrica),
        headers: {
          [HEADERS.TIPO]: "metrica.venta.actualizada",
          [HEADERS.ORIGEN]: ORIGEN,
        },
      },
    ],
  });

  console.log(
    `[KAFKA] Métrica publicada ${metrica.productoId} | topic=${TOPICS.METRICAS} partition=${metadata.partition} offset=${metadata.baseOffset}`,
  );
  return metadata;
}

// Manda al Dead Letter Topic el mensaje original tal cual llegó, con cabeceras
// de diagnóstico: por qué murió, cuántos intentos hubo y de dónde venía. Así
// el mensaje se puede reprocesar más tarde sin haber perdido su contexto.
export async function publicarEnDlt({ message, contexto, motivo, intentos, error }) {
  const producer = getProducer();
  if (!producer) throw new Error("El productor de Kafka no está conectado");

  const [metadata] = await producer.send({
    topic: TOPICS.DLT,
    messages: [
      {
        key: message.key,
        // El value viaja SIN TOCAR: si el JSON estaba corrupto, se conserva el
        // original para poder diagnosticarlo.
        value: message.value,
        headers: {
          ...message.headers,
          [HEADERS.MOTIVO]: motivo,
          [HEADERS.INTENTOS]: String(intentos),
          [HEADERS.ULTIMO_ERROR]: error ?? "",
          [HEADERS.TOPIC_ORIGEN]: contexto.topic,
          [HEADERS.PARTICION_ORIGEN]: String(contexto.partition),
          [HEADERS.OFFSET_ORIGEN]: String(contexto.offset),
          [HEADERS.GRUPO_ORIGEN]: contexto.groupId,
        },
      },
    ],
  });

  return metadata;
}
