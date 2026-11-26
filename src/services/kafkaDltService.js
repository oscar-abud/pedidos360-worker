import { supabase } from "../config/supabaseClient.js";
import { RetryableError } from "../messaging/errors.js";

// Dominio: FALLOS DE KAFKA. Persiste en Supabase los mensajes que terminaron
// en el Dead Letter Topic, para que el panel del Admin los muestre junto a los
// de la DLQ de RabbitMQ.
//
// Reutiliza la tabla mensajes_dlq que ya existía, con la columna "origen"
// para distinguir de dónde viene cada fallo ('rabbitmq' o 'kafka'). Así el
// panel no necesita dos tablas ni dos consultas.

export async function registrar(registro) {
  const { error } = await supabase.from("mensajes_dlq").insert({
    evento_id: registro.eventoId,
    tipo: registro.tipo,
    // En Kafka el equivalente a la cola de origen es el tópico y la partición
    // de donde salió el mensaje.
    cola_origen: registro.origen,
    motivo: registro.motivo,
    intentos: registro.intentos,
    error: registro.error,
    payload: registro.payload,
    origen: "kafka",
  });

  if (error) {
    throw new RetryableError(`No se pudo guardar en mensajes_dlq: ${error.message}`);
  }
}
