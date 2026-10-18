import { QUEUES } from "../config/messaging.js";
import { registerListener } from "../messaging/listener.js";
import * as AuditoriaService from "../services/auditoriaService.js";

// Dominio: AUDITORÍA. Escucha pedidos.auditoria.queue (binding "#": todos los
// eventos) y deja el historial en la tabla eventos.
export function registerAuditoriaConsumer(channel) {
  return registerListener(channel, QUEUES.AUDITORIA, async (evento, msg) => {
    await AuditoriaService.registrarEvento(evento, msg.fields.routingKey);
    console.log(`[AUDITORIA] ${evento.tipo} ${evento.id} registrado`);
  });
}
