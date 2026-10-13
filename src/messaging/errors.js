// Clasificación de errores de procesamiento. El listener decide la ruta del
// mensaje según el tipo de error:
//
//   RetryableError    -> falla transitoria (BD caída, timeout). Se reintenta
//                        con backoff; si se agotan los intentos, NACK -> DLQ.
//   NonRetryableError -> el mensaje nunca podrá procesarse (JSON inválido,
//                        datos faltantes). Va directo a la DLQ, sin reintentos.
//
// Cualquier otro error inesperado se trata como recuperable (conservador:
// mejor reintentar que perder el mensaje).

export class RetryableError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = "RetryableError";
  }
}

export class NonRetryableError extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = "NonRetryableError";
  }
}
