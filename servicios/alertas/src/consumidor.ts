import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Consumer, Producer, logLevel } from 'kafkajs';
import Redis from 'ioredis';
import { Pool } from 'pg';
import { hostname } from 'os';

export const metricas = {
  instancia: hostname(),
  incidencias_creadas: 0,
  incidencias_cerradas: 0,
  duplicados_ignorados: 0,
  notificaciones_generadas: 0,
};

const TIPOS = ['PROTESTA', 'CIERRE', 'RIÑA', 'HURTO', 'EMERGENCIA_MEDICA', 'OTRO'];
const MINUTOS_EXPIRACION = 30;

@Injectable()
export class ConsumidorAlertas implements OnModuleInit, OnModuleDestroy {
  kafka = new Kafka({
    clientId: `alertas-${hostname()}`,
    brokers: process.env.KAFKA_BROKERS.split(','),
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  });
  consumidor: Consumer = this.kafka.consumer({ groupId: 'servicio-alertas', sessionTimeout: 10000, heartbeatInterval: 2000 });
  productor: Producer = this.kafka.producer({ allowAutoTopicCreation: false });
  redis = new Redis({ host: process.env.REDIS_HOST, password: process.env.REDIS_PASSWORD, maxRetriesPerRequest: 2 });
  pool = new Pool({
    host: process.env.POSTGRES_HOST, database: process.env.POSTGRES_DB,
    user: process.env.POSTGRES_USER, password: process.env.POSTGRES_PASSWORD,
    max: 5, connectionTimeoutMillis: 3000,
  });
  temporizador: NodeJS.Timeout;

  async onModuleInit() {
    this.pool.on('error', (e) => console.warn('PostgreSQL:', e.message));
    await this.reconstruirActivasEnRedis();
    await this.productor.connect();
    await this.consumidor.connect();
    await this.consumidor.subscribe({ topic: 'incidencias.reportadas', fromBeginning: false });

    // Si el procesamiento lanza un error, el offset no se confirma y Kafka vuelve a entregar el mensaje
    await this.consumidor.run({
      eachMessage: async ({ message }) => {
        const mensaje = JSON.parse(message.value.toString());
        if (mensaje.accion === 'CERRAR') {
          await this.cerrarIncidencia(mensaje.id, mensaje.ts_emision);
          return;
        }
        if (!TIPOS.includes(mensaje.tipo)) return;

        const { rows: estaciones } = await this.pool.query(
          'SELECT id, nombre, zona, orden, id_corredor FROM estacion WHERE id = $1', [mensaje.id_estacion],
        );
        const estacion = estaciones[0];
        if (!estacion) return;

        // ON CONFLICT hace la operación idempotente: si el mensaje llega dos veces solo se inserta una
        const { rows } = await this.pool.query(
          `INSERT INTO incidencia (id, id_estacion, tipo, descripcion, origen, zona, ts)
           VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7 / 1000.0))
           ON CONFLICT (id) DO NOTHING RETURNING *`,
          [mensaje.id, estacion.id, mensaje.tipo, (mensaje.descripcion || '').slice(0, 280), mensaje.origen, estacion.zona, mensaje.ts_emision],
        );
        if (!rows[0]) {
          metricas.duplicados_ignorados++;
          return;
        }
        const incidencia = { ...rows[0], nombre_estacion: estacion.nombre };
        await this.redis.hset('incidencias:activas', incidencia.id, JSON.stringify(incidencia));

        // Notifica a quienes siguen una ruta que pasa por la estación afectada
        const texto = `${mensaje.tipo.replace('_', ' ')} en ${estacion.nombre} (zona ${estacion.zona})`;
        const { rows: destinatarios } = await this.pool.query(
          `WITH nuevas AS (
             INSERT INTO notificacion (id_suscripcion, id_incidencia, mensaje)
             SELECT s.id, $1, $2 FROM suscripcion s JOIN ruta r ON r.id = s.id_ruta
             WHERE s.activa AND r.id_corredor = $3 AND $4 BETWEEN r.orden_desde AND r.orden_hasta
             ON CONFLICT DO NOTHING RETURNING id_suscripcion)
           SELECT s.id_usuario, r.codigo AS ruta FROM nuevas n
           JOIN suscripcion s ON s.id = n.id_suscripcion JOIN ruta r ON r.id = s.id_ruta`,
          [incidencia.id, texto, estacion.id_corredor, estacion.orden],
        );
        metricas.notificaciones_generadas += destinatarios.length;
        metricas.incidencias_creadas++;

        await this.productor.send({
          topic: 'alertas.emitidas',
          acks: -1,
          messages: [{
            key: estacion.id,
            value: JSON.stringify({
              tipo: 'INCIDENCIA_NUEVA', incidencia, mensaje: texto, destinatarios,
              ts_emision: mensaje.ts_emision, ts_procesado: Date.now(), instancia_alertas: hostname(),
            }),
          }],
        });
      },
    });

    // Cierra automáticamente las incidencias abiertas hace más de 30 minutos
    this.temporizador = setInterval(async () => {
      const { rows } = await this.pool
        .query(`SELECT id FROM incidencia WHERE estado = 'ABIERTA' AND ts < now() - interval '${MINUTOS_EXPIRACION} minutes'`)
        .catch(() => ({ rows: [] }));
      for (const fila of rows) await this.cerrarIncidencia(fila.id, Date.now());
    }, 60000);
  }

  // El UPDATE con "estado = 'ABIERTA'" es atómico: si dos réplicas intentan cerrar la misma incidencia, solo una lo logra
  async cerrarIncidencia(id: string, tsEmision: number) {
    const { rows } = await this.pool.query(
      `UPDATE incidencia SET estado = 'CERRADA', cerrada_en = now() WHERE id::text = $1 AND estado = 'ABIERTA' RETURNING *`, [id],
    );
    if (!rows[0]) return;
    await this.redis.hdel('incidencias:activas', id);
    metricas.incidencias_cerradas++;
    await this.productor.send({
      topic: 'alertas.emitidas',
      acks: -1,
      messages: [{
        key: rows[0].id_estacion,
        value: JSON.stringify({ tipo: 'INCIDENCIA_CERRADA', incidencia: rows[0], ts_emision: tsEmision, ts_procesado: Date.now() }),
      }],
    });
  }

  // Recuperación: si Redis perdió sus datos, las incidencias activas se reconstruyen desde PostgreSQL
  async reconstruirActivasEnRedis() {
    const { rows } = await this.pool.query(
      `SELECT i.*, e.nombre AS nombre_estacion FROM incidencia i JOIN estacion e ON e.id = i.id_estacion WHERE i.estado = 'ABIERTA'`,
    );
    await this.redis.del('incidencias:activas');
    for (const fila of rows) await this.redis.hset('incidencias:activas', fila.id, JSON.stringify(fila));
    console.log(`Incidencias activas reconstruidas en Redis: ${rows.length}`);
  }

  async onModuleDestroy() {
    clearInterval(this.temporizador);
    await this.consumidor.disconnect();
    await this.productor.disconnect();
  }
}
