import { HEADERS } from "../../config/kafka.js";
import { NonRetryableError, RetryableError } from "../../messaging/errors.js";

// Solo para la demo: el panel del Admin publica eventos con el header
// simular-fallo para mostrar cada ruta de error en vivo, igual que ya se hace
// con RabbitMQ (consumers/simulacion.js).
//
//   transitorio -> 3 reintentos con backoff (1s, 2s, 4s) y después al DLT
//   permanente  -> al DLT directo, sin reintentos
export function aplicarFalloSimulado(contexto) {
  const fallo = contexto.headers?.[HEADERS.SIMULAR_FALLO];

  if (fallo === "transitorio") {
    throw new RetryableError("Fallo transitorio simulado (ej: base de datos no disponible)");
  }
  if (fallo === "permanente") {
    throw new NonRetryableError("Fallo permanente simulado (ej: datos del pedido inválidos)");
  }
}
