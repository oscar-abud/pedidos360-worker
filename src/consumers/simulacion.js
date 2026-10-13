import { HEADERS } from "../config/messaging.js";
import { NonRetryableError, RetryableError } from "../messaging/errors.js";

// Solo para la demo: el panel del Admin publica eventos con el header
// x-simular-fallo para mostrar cada ruta de error en vivo.
export function aplicarFalloSimulado(msg) {
  const fallo = msg.properties.headers?.[HEADERS.SIMULAR_FALLO];
  if (fallo === "transitorio") {
    throw new RetryableError("Fallo transitorio simulado (ej: base de datos no disponible)");
  }
  if (fallo === "permanente") {
    throw new NonRetryableError("Fallo permanente simulado (ej: datos del pedido inválidos)");
  }
}
