import { NonRetryableError } from "../messaging/errors.js";

// Arma y "envía" la notificación de cada evento. El envío se simula con un
// log (aquí se integraría un proveedor de correo como SendGrid/SES).

const PLANTILLAS = {
  "pedido.creado": (data) => ({
    para: data.clienteNombre,
    asunto: `Recibimos tu pedido ${data.id}`,
  }),
  "pedido.estado.actualizado": (data) => ({
    para: data.clienteNombre,
    asunto: `Tu pedido ${data.id} ahora está "${data.estado}"`,
  }),
  "usuario.creado": (data) => ({
    para: data.email,
    asunto: `Bienvenido/a a Pedidos360, ${data.nombre}`,
  }),
};

export function notificar(evento) {
  const plantilla = PLANTILLAS[evento?.tipo];
  if (!plantilla) {
    throw new NonRetryableError(`Tipo de evento sin plantilla de notificación: ${evento?.tipo}`);
  }
  if (!evento.data) {
    throw new NonRetryableError(`El evento ${evento.id} no trae datos`);
  }

  const { para, asunto } = plantilla(evento.data);
  console.log(`[NOTIFICACION] ✉ Para: ${para} | Asunto: ${asunto}`);
  return { para, asunto };
}
