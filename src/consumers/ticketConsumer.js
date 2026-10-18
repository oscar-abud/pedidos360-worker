import { QUEUES } from "../config/messaging.js";
import { registerListener } from "../messaging/listener.js";
import * as TicketService from "../services/ticketService.js";
import { aplicarFalloSimulado } from "./simulacion.js";

// Dominio: TICKETS. Escucha pedidos.tickets.queue (pedido.creado) y genera
// el comprobante del pedido.
export function registerTicketConsumer(channel) {
  return registerListener(channel, QUEUES.TICKETS, async (evento, msg) => {
    aplicarFalloSimulado(msg);
    const ticket = await TicketService.generarTicket(evento.data);
    console.log(`[TICKET] ${ticket.numero} generado para el pedido ${ticket.pedido_id}`);
  });
}
