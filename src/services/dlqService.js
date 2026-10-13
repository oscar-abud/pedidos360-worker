import { supabase } from "../config/supabaseClient.js";

// Persiste los mensajes muertos para auditoría (panel DLQ del front).
export async function registrar(registro) {
  const { error } = await supabase.from("mensajes_dlq").insert({
    evento_id: registro.eventoId,
    tipo: registro.tipo,
    cola_origen: registro.colaOrigen,
    motivo: registro.motivo,
    intentos: registro.intentos,
    error: registro.error,
    payload: registro.payload,
  });
  if (error) {
    throw new Error(`No se pudo guardar en mensajes_dlq: ${error.message}`);
  }
}
