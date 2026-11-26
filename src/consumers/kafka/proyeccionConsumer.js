import { CONSUMER_GROUPS, TOPICS } from "../../config/kafka.js";
import { registerKafkaListener } from "../../messaging/kafkaListener.js";
import * as ProyeccionService from "../../services/proyeccionService.js";

// Dominio: PROYECCION. Grupo pedidos360-proyeccion sobre pedidos.metricas.v1.
//
// El topico esta COMPACTADO: conserva el ultimo valor de cada productoId para
// siempre. Por eso se lee desde el principio en cada arranque y el ranking se
// reconstruye entero sin tocar la base de datos. El log ES el estado.
//
// validar: false -> estos mensajes son metricas, no eventos de dominio, asi
// que no les aplica el contrato de events/eventSchemas.js.
export function registerProyeccionConsumer() {
  return registerKafkaListener(
    {
      groupId: CONSUMER_GROUPS.PROYECCION,
      topic: TOPICS.METRICAS,
      fromBeginning: true,
      validar: false,
    },
    async (metrica, contexto) => {
      const actualizada = ProyeccionService.registrarMetrica(metrica);
      console.log(
        `[PROYECCION] ${actualizada.nombre}: ${actualizada.unidades} unidades / ${actualizada.monto} (key=${contexto.key})`,
      );
    },
  );
}
