import { supabase } from "../config/supabaseClient.js";
import { NonRetryableError, RetryableError } from "../messaging/errors.js";

// Genera el ticket/comprobante de un pedido recién creado. Idempotente:
// pedido_id es UNIQUE, así que si el mismo evento se procesa dos veces
// (por un reintento) no se duplica el ticket.

function numeroTicket(pedido) {
  const fecha = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  return `TCK-${fecha}-${pedido.id.slice(0, 8).toUpperCase()}`;
}

export async function generarTicket(pedido) {
  if (!pedido?.id || !pedido?.clienteNombre) {
    throw new NonRetryableError("El evento pedido.creado no trae id o clienteNombre");
  }

  const ticket = {
    numero: numeroTicket(pedido),
    pedido_id: pedido.id,
    cliente_nombre: pedido.clienteNombre,
    total: Number(pedido.total) || 0,
  };

  const { error } = await supabase
    .from("tickets")
    .upsert(ticket, { onConflict: "pedido_id", ignoreDuplicates: true });

  if (error) {
    // Falla de BD/red: puede resolverse sola, se reintenta.
    throw new RetryableError(`No se pudo guardar el ticket: ${error.message}`);
  }
  return ticket;
}
