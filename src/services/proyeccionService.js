// Dominio: PROYECCIÓN DE MÉTRICAS. Mantiene en memoria el ranking de productos
// más vendidos a partir del tópico compactado pedidos.metricas.v1.
// No conoce Kafka: el consumidor le entrega cada métrica ya deserializada.
//
// Por qué esto demuestra para qué sirve Kafka:
//
// El tópico está COMPACTADO por productoId, así que conserva el último valor
// de cada producto para siempre. Cuando el worker arranca, el consumidor lee
// el tópico DESDE EL PRINCIPIO y reconstruye el ranking completo sin consultar
// la base de datos: el log ES el estado. Con RabbitMQ esto no se puede hacer,
// porque un mensaje desaparece en cuanto alguien le hace ACK.

const porProducto = new Map();
let ultimaActualizacion = null;

export function registrarMetrica(metrica) {
  // El tópico compactado garantiza que el último mensaje de cada key es el
  // valor vigente, así que se reemplaza en vez de sumar.
  porProducto.set(metrica.productoId, {
    productoId: metrica.productoId,
    nombre: metrica.nombre,
    unidades: metrica.unidades,
    monto: metrica.monto,
    actualizadoEn: metrica.actualizadoEn,
  });

  ultimaActualizacion = new Date().toISOString();
  return porProducto.get(metrica.productoId);
}

export function getRanking(limite = 5) {
  const productos = [...porProducto.values()].sort((a, b) => b.unidades - a.unidades);

  return {
    productos: productos.slice(0, limite),
    totalProductos: porProducto.size,
    unidadesTotales: productos.reduce((total, p) => total + p.unidades, 0),
    montoTotal: productos.reduce((total, p) => total + p.monto, 0),
    ultimaActualizacion,
  };
}
