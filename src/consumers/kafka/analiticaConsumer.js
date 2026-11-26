import { CONSUMER_GROUPS, TOPICS } from "../../config/kafka.js";
import { registerKafkaListener } from "../../messaging/kafkaListener.js";
import * as AnaliticaService from "../../services/analiticaService.js";
import * as KafkaProducer from "../../messaging/kafkaProducer.js";
import { aplicarFalloSimulado } from "./simulacion.js";

// Dominio: ANALITICA. Grupo pedidos360-analitica sobre pedidos.eventos.v1.
//
// Lee el mismo topico que el grupo pedidos360-back-cache del back-principal:
// al ser grupos distintos, Kafka entrega una copia a cada uno y cada uno lleva
// su propio offset. Dentro de este grupo, en cambio, las 3 particiones se
// reparten entre sus miembros: levantar un segundo worker reparte la carga.
//
// fromBeginning: true -> al arrancar procesa el historico retenido. Es seguro
// porque analiticaService es idempotente (no vuelve a sumar un evento ya visto).
export function registerAnaliticaConsumer() {
  return registerKafkaListener(
    {
      groupId: CONSUMER_GROUPS.ANALITICA,
      topic: TOPICS.EVENTOS,
      fromBeginning: true,
    },
    async (evento, contexto) => {
      aplicarFalloSimulado(contexto);

      const metricas = await AnaliticaService.procesarEvento(evento);

      // Cada acumulado se publica en el topico compactado de metricas: ahi lo
      // recoge el grupo pedidos360-proyeccion. El worker es consumidor y
      // productor a la vez, que es como se encadenan los pasos de un pipeline
      // de streaming.
      for (const metrica of metricas) {
        await KafkaProducer.publicarMetrica(metrica);
      }

      if (metricas.length > 0) {
        console.log(
          `[ANALITICA] ${evento.tipo} ${evento.id}: ${metricas.length} producto(s) actualizados`,
        );
      }
    },
  );
}
