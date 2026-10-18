import { supabase } from "../config/supabaseClient.js";
import { NonRetryableError, RetryableError } from "../messaging/errors.js";

// Registro de auditoría: guarda CADA evento de dominio que pasa por el
// sistema (historial completo, no solo los fallos). Idempotente: evento_id es
// UNIQUE, así un reintento no duplica la fila.
export async function registrarEvento(evento, routingKey) {
  if (!evento?.id || !evento?.tipo) {
    throw new NonRetryableError("El evento no trae id o tipo");
  }

  const { error } = await supabase.from("eventos").upsert(
    {
      evento_id: evento.id,
      tipo: evento.tipo,
      routing_key: routingKey,
      data: evento.data ?? null,
      ocurrido_en: evento.ocurridoEn ?? null,
    },
    { onConflict: "evento_id", ignoreDuplicates: true },
  );

  if (error) {
    throw new RetryableError(`No se pudo guardar el evento: ${error.message}`);
  }
}
