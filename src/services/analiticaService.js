import { supabase } from "../config/supabaseClient.js";
import { NonRetryableError, RetryableError } from "../messaging/errors.js";
import { TIPOS_EVENTO } from "../events/eventSchemas.js";

// Dominio: ANALÍTICA DE VENTAS. Recibe eventos de pedidos ya validados y
// acumula, por producto, cuántas unidades se han vendido y por cuánto dinero.
// No conoce Kafka: el consumidor le pasa el evento y publica lo que devuelve.
//
// Idempotencia: acumular NO es una operación repetible (sumar dos veces el
// mismo pedido daría el doble). Por eso cada evento se registra primero en
// eventos_analitica, cuya clave primaria es el id del evento: si el mensaje se
// reprocesa (un reintento, un reset de offsets, un rebalanceo), el insert
// choca con la clave y el evento se ignora en vez de duplicar las ventas.

const CODIGO_DUPLICADO = "23505"; // unique_violation de PostgreSQL

// Devuelve true si el evento es nuevo, false si ya se había procesado.
async function marcarComoProcesado(eventoId) {
  const { error } = await supabase
    .from("eventos_analitica")
    .insert({ evento_id: eventoId });

  if (!error) return true;
  if (error.code === CODIGO_DUPLICADO) return false;

  // Falla de BD o de red: puede resolverse sola, se reintenta.
  throw new RetryableError(`No se pudo registrar el evento analítico: ${error.message}`);
}

// Acumula la venta de un item de forma ATÓMICA en la base de datos. Se usa una
// función de PostgreSQL (registrar_venta) en vez de leer-sumar-escribir: con
// varias particiones procesándose a la vez, dos workers podrían leer el mismo
// total y pisarse el uno al otro.
async function acumularVenta(item) {
  const { data, error } = await supabase.rpc("registrar_venta", {
    p_producto_id: item.productoId,
    p_nombre: item.nombre,
    p_unidades: item.cantidad,
    p_monto: Number(item.precioUnitario) * item.cantidad,
  });

  if (error) {
    throw new RetryableError(`No se pudo acumular la venta: ${error.message}`);
  }
  if (!data) {
    throw new NonRetryableError(
      `La función registrar_venta no devolvió datos para el producto ${item.productoId}`,
    );
  }

  const fila = Array.isArray(data) ? data[0] : data;
  return {
    productoId: fila.producto_id,
    nombre: fila.nombre,
    unidades: fila.unidades,
    monto: Number(fila.monto),
    actualizadoEn: fila.actualizado_en,
  };
}

/**
 * Procesa un evento de dominio y devuelve las métricas que cambiaron, para que
 * el consumidor las publique en el tópico de métricas.
 */
export async function procesarEvento(evento) {
  // El grupo de analítica recibe TODOS los eventos del tópico, pero solo los
  // pedidos creados mueven las ventas. Los demás se dan por procesados.
  if (evento.tipo !== TIPOS_EVENTO.PEDIDO_CREADO) {
    return [];
  }

  const items = evento.data.items ?? [];
  if (items.length === 0) {
    // Pedido sin items (por ejemplo, el evento de demo del panel): nada que
    // acumular, pero tampoco es un error.
    return [];
  }

  const esNuevo = await marcarComoProcesado(evento.id);
  if (!esNuevo) {
    console.log(`[ANALITICA] El evento ${evento.id} ya estaba procesado, se ignora`);
    return [];
  }

  const metricas = [];
  for (const item of items) {
    metricas.push(await acumularVenta(item));
  }
  return metricas;
}
