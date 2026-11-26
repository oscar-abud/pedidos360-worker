// Contrato de los eventos de dominio que viajan por Kafka (DTOs validados).
//
// IMPORTANTE: este archivo es un CONTRATO COMPARTIDO con el back-principal.
// Como son repositorios separados (no hay monorepo), existe una copia idéntica
// en EP1_CloudNative-brack-express/src/events/eventSchemas.js. Si cambias un
// esquema acá, tienes que cambiarlo también allá, o este consumidor empezará a
// mandar al DLT eventos que en realidad son válidos.
//
// El envelope es el mismo que ya se usaba con RabbitMQ, así ambos transportes
// hablan exactamente el mismo idioma:
//
//   { id, tipo, ocurridoEn, data }
//
// Validar en el productor evita publicar basura; validar en el consumidor evita
// que un mensaje mal formado detenga el procesamiento (se manda al DLT).

import { z } from "zod";

export const TIPOS_EVENTO = {
  PEDIDO_CREADO: "pedido.creado",
  PEDIDO_ESTADO_ACTUALIZADO: "pedido.estado.actualizado",
  USUARIO_CREADO: "usuario.creado",
};

// Los esquemas de data usan .passthrough(): se exigen los campos que el
// consumidor necesita, pero se dejan pasar los extra. Así, agregar un campo
// al modelo de pedidos no rompe el stream ni obliga a desplegar todo a la vez.

const itemSchema = z
  .object({
    productoId: z.string().uuid("productoId debe ser un UUID"),
    nombre: z.string().min(1, "El item necesita nombre"),
    cantidad: z.number().int().positive("La cantidad debe ser mayor a 0"),
    precioUnitario: z.coerce.number().nonnegative("El precio no puede ser negativo"),
  })
  .passthrough();

const pedidoCreadoData = z
  .object({
    id: z.string().uuid("El id del pedido debe ser un UUID"),
    clienteNombre: z.string().min(1, "El pedido necesita clienteNombre"),
    total: z.coerce.number().nonnegative("El total no puede ser negativo").default(0),
    estado: z.string().optional(),
    items: z.array(itemSchema).default([]),
  })
  .passthrough();

const pedidoEstadoActualizadoData = z
  .object({
    id: z.string().uuid("El id del pedido debe ser un UUID"),
    clienteNombre: z.string().min(1, "El evento necesita clienteNombre"),
    estado: z.string().min(1, "El evento necesita estado"),
  })
  .passthrough();

const usuarioCreadoData = z
  .object({
    id: z.string().min(1, "El usuario necesita id"),
    nombre: z.string().min(1, "El usuario necesita nombre"),
    email: z.string().min(1, "El usuario necesita email"),
    roles: z.array(z.string()).default([]),
  })
  .passthrough();

const envelope = (tipo, dataSchema) =>
  z.object({
    id: z.string().uuid("El id del evento debe ser un UUID"),
    tipo: z.literal(tipo),
    ocurridoEn: z.string().min(1, "El evento necesita ocurridoEn"),
    data: dataSchema,
  });

// discriminatedUnion por "tipo": zod elige el esquema de data según el tipo de
// evento y, si el tipo no es ninguno de los tres, lo rechaza con un mensaje claro.
export const eventoSchema = z.discriminatedUnion("tipo", [
  envelope(TIPOS_EVENTO.PEDIDO_CREADO, pedidoCreadoData),
  envelope(TIPOS_EVENTO.PEDIDO_ESTADO_ACTUALIZADO, pedidoEstadoActualizadoData),
  envelope(TIPOS_EVENTO.USUARIO_CREADO, usuarioCreadoData),
]);

// Devuelve { exito, evento } o { exito: false, errores }. No lanza: cada lado
// decide qué hacer (el productor no publica, el consumidor manda al DLT).
export function parseEvento(valor) {
  const resultado = eventoSchema.safeParse(valor);
  if (resultado.success) {
    return { exito: true, evento: resultado.data };
  }
  return {
    exito: false,
    errores: resultado.error.issues.map(
      (issue) => `${issue.path.join(".") || "evento"}: ${issue.message}`,
    ),
  };
}

// La key decide la partición: todos los eventos de un mismo pedido caen en la
// misma y Kafka garantiza su orden relativo. Sin key, el reparto sería rotativo
// y "pedido.creado" podría procesarse después de su cambio de estado.
export function claveDeEvento(evento) {
  return evento?.data?.id ?? evento?.id ?? null;
}
