import { QUEUES } from "../config/messaging.js";
import { registerListener } from "../messaging/listener.js";
import * as NotificacionService from "../services/notificacionService.js";

// Dominio: NOTIFICACIONES. Escucha pedidos.notificaciones.queue
// (pedido.creado, pedido.estado.*, usuario.creado) y avisa al destinatario.
export function registerNotificacionConsumer(channel) {
  return registerListener(channel, QUEUES.NOTIFICACIONES, async (evento) => {
    NotificacionService.notificar(evento);
  });
}
