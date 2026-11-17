import { Body, Controller, Delete, Get, HttpCode, HttpException, Param, Patch, Post, Query, Req } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { kafka, productor, redis, pool, metricas, Roles, TODOS_LOS_ROLES } from './comun';
import { circuitoAuth } from './proxy.controller';

const TIPOS_INCIDENCIA = ['PROTESTA', 'CIERRE', 'RIÑA', 'HURTO', 'EMERGENCIA_MEDICA', 'OTRO'];

// Catálogo (estaciones y rutas) en memoria: cambia muy poco, así se evita consultar la base en cada petición
let estaciones: any[] = [];
let rutas: any[] = [];

// Estado de los componentes distribuidos, recalculado cada 3 s en segundo plano
const estadoSistema: any = { brokers: [], grupos: {}, redis: 'DESCONOCIDO', postgres: 'DESCONOCIDO', actualizado: null };
const admin = kafka.admin();
let adminConectado = false;

setInterval(async () => {
  if (estaciones.length === 0) {
    try {
      estaciones = (await pool.query('SELECT id, nombre, lat, lon, zona, orden FROM estacion ORDER BY orden')).rows;
      rutas = (await pool.query('SELECT id, codigo, nombre, orden_desde, orden_hasta FROM ruta ORDER BY codigo')).rows;
    } catch {}
  }
  estadoSistema.redis = await redis.ping().then(() => 'ACTIVO').catch(() => 'CAIDO');
  estadoSistema.postgres = await pool.query('SELECT 1').then(() => 'ACTIVO').catch(() => 'CAIDO');
  try {
    if (!adminConectado) {
      await admin.connect();
      adminConectado = true;
    }
    estadoSistema.brokers = (await admin.describeCluster()).brokers.map((b) => b.host).sort();
  } catch {
    estadoSistema.brokers = [];
    adminConectado = false;
    await admin.disconnect().catch(() => {});
  }
  // Los grupos se consultan aparte: un error transitorio aquí no debe ocultar el estado de los brokers
  try {
    const descripcion = await admin.describeGroups(['servicio-ocupacion', 'servicio-alertas']);
    for (const [grupo, topic] of [['servicio-ocupacion', 'ocupacion.eventos'], ['servicio-alertas', 'incidencias.reportadas']]) {
      const confirmados = (await admin.fetchOffsets({ groupId: grupo, topics: [topic] }))[0].partitions;
      const finales = await admin.fetchTopicOffsets(topic);
      // Lag = mensajes publicados que el grupo todavía no ha procesado
      const lag = finales.reduce((suma, p) => {
        const confirmado = Number(confirmados.find((c) => c.partition === p.partition)?.offset ?? -1);
        return suma + Math.max(0, Number(p.offset) - Math.max(confirmado, 0));
      }, 0);
      const miembros = descripcion.groups.find((g) => g.groupId === grupo)?.members.length ?? 0;
      estadoSistema.grupos[grupo] = { consumidores: miembros, lag };
    }
  } catch {}
  estadoSistema.actualizado = new Date().toISOString();
}, 3000);

@Controller()
export class ApiController {
  @Get('health')
  salud() {
    return { servicio: 'gateway', estado: 'activo', instancia: metricas.instancia };
  }

  @Get('estaciones')
  @Roles(...TODOS_LOS_ROLES)
  listarEstaciones() {
    return estaciones;
  }

  @Get('rutas')
  @Roles(...TODOS_LOS_ROLES)
  listarRutas() {
    return rutas;
  }

  // Estado en vivo de todos los buses (se lee de Redis, no de PostgreSQL)
  @Get('ocupacion')
  @Roles(...TODOS_LOS_ROLES)
  async ocupacion(@Query('ruta') ruta?: string) {
    const ids = await redis.smembers('vehiculos');
    const datos = ids.length ? await redis.pipeline(ids.map((id) => ['hget', `vehiculo:${id}`, 'datos'])).exec() : [];
    const vehiculos = datos
      .filter(([, valor]) => valor)
      .map(([, valor]) => JSON.parse(valor as string))
      .filter((v) => !ruta || v.ruta === ruta);
    return vehiculos.sort((a, b) => a.id_vehiculo.localeCompare(b.id_vehiculo));
  }

  @Get('ocupacion/ruta/:codigo')
  @Roles(...TODOS_LOS_ROLES)
  async ocupacionPorRuta(@Param('codigo') codigo: string) {
    if (!rutas.some((r) => r.codigo === codigo)) throw new HttpException({ error: 'Ruta no encontrada' }, 404);
    const vehiculos = await this.ocupacion(codigo);
    const resumen = { BAJO: 0, MEDIO: 0, ALTO: 0 };
    vehiculos.forEach((v) => resumen[v.nivel]++);
    return { ruta: codigo, resumen, vehiculos };
  }

  // Responde "¿qué tan lleno viene el próximo bus?" para una estación
  @Get('ocupacion/estacion/:id')
  @Roles(...TODOS_LOS_ROLES)
  async ocupacionPorEstacion(@Param('id') id: string) {
    const estacion = estaciones.find((e) => e.id === id);
    if (!estacion) throw new HttpException({ error: 'Estación no encontrada' }, 404);
    const ordenPorId = Object.fromEntries(estaciones.map((e) => [e.id, e.orden]));
    const proximos = (await this.ocupacion())
      .map((v) => ({ ...v, estaciones_de_distancia: Math.abs(ordenPorId[v.id_estacion] - estacion.orden) }))
      .filter((v) => (v.sentido === 'SUR' ? ordenPorId[v.id_estacion] <= estacion.orden : ordenPorId[v.id_estacion] >= estacion.orden))
      .sort((a, b) => a.estaciones_de_distancia - b.estaciones_de_distancia);
    return {
      estacion,
      proximos_hacia_el_sur: proximos.filter((v) => v.sentido === 'SUR').slice(0, 5),
      proximos_hacia_el_norte: proximos.filter((v) => v.sentido === 'NORTE').slice(0, 5),
    };
  }

  @Get('incidencias')
  @Roles(...TODOS_LOS_ROLES)
  async listarIncidencias(@Query('estado') estado?: string) {
    if (estado === 'todas') {
      const { rows } = await pool.query(
        `SELECT i.*, e.nombre AS nombre_estacion FROM incidencia i JOIN estacion e ON e.id = i.id_estacion ORDER BY i.ts DESC LIMIT 100`,
      );
      return rows;
    }
    const activas = await redis.hvals('incidencias:activas');
    return activas.map((texto) => JSON.parse(texto)).sort((a, b) => (a.ts < b.ts ? 1 : -1));
  }

  // El gateway no escribe la incidencia: la publica en Kafka y el servicio de Alertas la procesa (202 Accepted)
  @Post('incidencias')
  @HttpCode(202)
  @Roles('CIUDADANO', 'OPERADOR')
  async reportarIncidencia(@Body() cuerpo: any, @Req() peticion: any) {
    if (!TIPOS_INCIDENCIA.includes(cuerpo?.tipo)) {
      throw new HttpException({ error: `tipo debe ser uno de: ${TIPOS_INCIDENCIA.join(', ')}` }, 400);
    }
    if (!estaciones.some((e) => e.id === cuerpo?.id_estacion)) throw new HttpException({ error: 'id_estacion inválido' }, 400);
    if (cuerpo.descripcion !== undefined && (typeof cuerpo.descripcion !== 'string' || cuerpo.descripcion.length > 280)) {
      throw new HttpException({ error: 'descripcion debe ser texto de máximo 280 caracteres' }, 400);
    }
    const mensaje = {
      accion: 'CREAR',
      id: randomUUID(),
      id_estacion: cuerpo.id_estacion,
      tipo: cuerpo.tipo,
      descripcion: (cuerpo.descripcion || '').replace(/[<>]/g, ''),
      origen: peticion.usuario.rol === 'CIUDADANO' ? 'CIUDADANO' : 'OPERADOR',
      ts_emision: Date.now(),
    };
    try {
      await productor.send({ topic: 'incidencias.reportadas', acks: -1, messages: [{ key: mensaje.id_estacion, value: JSON.stringify(mensaje) }] });
    } catch {
      throw new HttpException({ error: 'Broker de eventos no disponible, intente de nuevo' }, 503);
    }
    return { id: mensaje.id, estado: 'EN_PROCESO' };
  }

  @Patch('incidencias/:id/cerrar')
  @HttpCode(202)
  @Roles('OPERADOR', 'ADMINISTRADOR')
  async cerrarIncidencia(@Param('id') id: string) {
    if (!(await redis.hexists('incidencias:activas', id))) throw new HttpException({ error: 'Incidencia activa no encontrada' }, 404);
    await productor.send({
      topic: 'incidencias.reportadas', acks: -1,
      messages: [{ key: id, value: JSON.stringify({ accion: 'CERRAR', id, ts_emision: Date.now() }) }],
    });
    return { id, estado: 'CIERRE_EN_PROCESO' };
  }

  @Get('suscripciones')
  @Roles('CIUDADANO')
  async listarSuscripciones(@Req() peticion: any) {
    const { rows } = await pool.query(
      `SELECT s.id, r.codigo AS ruta, r.nombre, s.activa, s.creado_en FROM suscripcion s
       JOIN ruta r ON r.id = s.id_ruta WHERE s.id_usuario = $1 AND s.activa ORDER BY r.codigo`,
      [peticion.usuario.sub],
    );
    return rows;
  }

  @Post('suscripciones')
  @Roles('CIUDADANO')
  async suscribir(@Body() cuerpo: any, @Req() peticion: any) {
    const ruta = rutas.find((r) => r.codigo === cuerpo?.ruta);
    if (!ruta) throw new HttpException({ error: 'Ruta inválida' }, 400);
    const { rows } = await pool.query(
      `INSERT INTO suscripcion (id_usuario, id_ruta) VALUES ($1, $2)
       ON CONFLICT (id_usuario, id_ruta) DO UPDATE SET activa = TRUE RETURNING id, activa`,
      [peticion.usuario.sub, ruta.id],
    );
    return { ...rows[0], ruta: ruta.codigo };
  }

  // Solo puede eliminar sus propias suscripciones (el filtro por id_usuario lo garantiza)
  @Delete('suscripciones/:id')
  @Roles('CIUDADANO')
  async eliminarSuscripcion(@Param('id') id: string, @Req() peticion: any) {
    const { rowCount } = await pool.query(
      `UPDATE suscripcion SET activa = FALSE WHERE id::text = $1 AND id_usuario = $2`, [id, peticion.usuario.sub],
    );
    if (!rowCount) throw new HttpException({ error: 'Suscripción no encontrada' }, 404);
    return { id, activa: false };
  }

  @Get('notificaciones')
  @Roles('CIUDADANO')
  async listarNotificaciones(@Req() peticion: any) {
    const { rows } = await pool.query(
      `SELECT n.id, n.mensaje, n.enviada_en, r.codigo AS ruta, i.estado AS estado_incidencia
       FROM notificacion n JOIN suscripcion s ON s.id = n.id_suscripcion JOIN ruta r ON r.id = s.id_ruta
       JOIN incidencia i ON i.id = n.id_incidencia
       WHERE s.id_usuario = $1 ORDER BY n.enviada_en DESC LIMIT 50`,
      [peticion.usuario.sub],
    );
    return rows;
  }

  // Promedio de ocupación agrupado por intervalos de tiempo (función time_bucket de TimescaleDB)
  @Get('historico/ocupacion')
  @Roles('OPERADOR', 'ADMINISTRADOR')
  async historico(@Query('ruta') ruta?: string, @Query('minutos') minutos = '60', @Query('intervalo') intervalo = '1') {
    const min = Number(minutos), int = Number(intervalo);
    if (!Number.isInteger(min) || min < 1 || min > 1440 || !Number.isInteger(int) || int < 1 || int > 60) {
      throw new HttpException({ error: 'minutos (1-1440) e intervalo (1-60) deben ser enteros' }, 400);
    }
    const { rows } = await pool.query(
      `SELECT time_bucket(make_interval(mins => $1), ts) AS intervalo,
              round(avg(ocupacion_pct), 1) AS ocupacion_promedio, max(ocupacion_pct) AS ocupacion_maxima,
              count(*) FILTER (WHERE nivel = 'ALTO') AS eventos_alto, count(*) AS eventos
       FROM evento_ocupacion
       WHERE ts > now() - make_interval(mins => $2) AND ($3::text IS NULL OR ruta = $3)
       GROUP BY 1 ORDER BY 1`,
      [int, min, ruta || null],
    );
    return rows;
  }

  @Get('metricas')
  @Roles('OPERADOR', 'ADMINISTRADOR')
  verMetricas() {
    const ordenadas = [...metricas.latencias_ms].sort((a, b) => a - b);
    const percentil = (p: number) => (ordenadas.length ? ordenadas[Math.min(ordenadas.length - 1, Math.floor(ordenadas.length * p))] : null);
    const hace10s = Date.now() - 10000;
    return {
      instancia_gateway: metricas.instancia,
      latencia_ms: { muestras: ordenadas.length, p50: percentil(0.5), p95: percentil(0.95), p99: percentil(0.99), max: ordenadas.at(-1) ?? null },
      eventos_por_segundo: metricas.recepciones.filter((t) => t > hace10s).length / 10,
      clientes_websocket: metricas.clientes_ws,
      circuito_auth: circuitoAuth.opened ? 'ABIERTO' : circuitoAuth.halfOpen ? 'SEMIABIERTO' : 'CERRADO',
      ...estadoSistema,
    };
  }
}
