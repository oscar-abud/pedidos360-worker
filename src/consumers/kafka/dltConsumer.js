import { CONSUMER_GROUPS, HEADERS, TOPICS } from "../../config/kafka.js";
import { registerKafkaListener } from "../../messaging/kafkaListener.js";
import * as KafkaDltService from "../../services/kafkaDltService.js";

// Dominio: FALLOS. Grupo pedidos360-dlt-monitor sobre pedidos.eventos.dlt.v1.
//
// Registra en el log y en Supabase por que murio cada mensaje, para que el
// panel del Admin lo muestre junto a los de la DLQ de RabbitMQ.
//
// validar: false    -> el payload puede estar roto a proposito, se guarda tal cual.
// enviarADlt: false -> no existe un "DLT del DLT". Si Supabase falla, se
//                      reintenta; si no se logra, el mensaje sigue retenido 30
//                      dias en el topico y puede reprocesarse con reset-offsets.
export function registerDltConsumer() {
  return registerKafkaListener(
    {
      groupId: CONSUMER_GROUPS.DLT_MONITOR,
      topic: TOPICS.DLT,
      fromBeginning: true,
      validar: false,
      enviarADlt: false,
    },
    async (payload, contexto) => {
      const headers = contexto.headers ?? {};
      const origen = `${headers[HEADERS.TOPIC_ORIGEN] ?? "?"}[${headers[HEADERS.PARTICION_ORIGEN] ?? "?"}]@${headers[HEADERS.OFFSET_ORIGEN] ?? "?"}`;

      console.error(
        `[DLT ${TOPICS.DLT}] Mensaje en cuarentena | evento=${headers[HEADERS.EVENTO_ID] ?? payload?.id ?? "?"} ` +
          `tipo=${headers[HEADERS.TIPO] ?? payload?.tipo ?? "?"} origen=${origen} ` +
          `grupo=${headers[HEADERS.GRUPO_ORIGEN] ?? "?"} motivo=${headers[HEADERS.MOTIVO] ?? "desconocido"} ` +
          `intentos=${headers[HEADERS.INTENTOS] ?? 0} error=${headers[HEADERS.ULTIMO_ERROR] || "-"}`,
      );

      await KafkaDltService.registrar({
        eventoId: headers[HEADERS.EVENTO_ID] ?? payload?.id ?? null,
        tipo: headers[HEADERS.TIPO] ?? payload?.tipo ?? null,
        origen,
        motivo: headers[HEADERS.MOTIVO] ?? "desconocido",
        intentos: Number(headers[HEADERS.INTENTOS] ?? 0),
        error: headers[HEADERS.ULTIMO_ERROR] || null,
        payload,
      });
    },
  );
}
