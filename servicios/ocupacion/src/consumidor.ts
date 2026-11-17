import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Consumer, Producer, logLevel } from 'kafkajs';
import Redis from 'ioredis';
import { Pool } from 'pg';
import CircuitBreaker from 'opossum';
import { hostname } from 'os';

export const metricas = {
  instancia: hostname(),
  procesados: 0,
  descartados_obsoletos: 0,
  enviados_a_pendientes: 0,
  recuperados_de_pendientes: 0,
  particiones_asignadas: [] as number[],
  circuito_postgres: 'CERRADO',
};

// Script atómico en Redis: solo actualiza si el evento es más reciente que el guardado.
// Evita que un mensaje repetido o atrasado (entrega "al menos una vez") sobrescriba un estado nuevo.
const SCRIPT_ULTIMO_GANA = `
local actual = redis.call('HGET', KEYS[1], 'ts_emision')
if actual and tonumber(actual) >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'ts_emision', ARGV[1], 'datos', ARGV[2])
redis.call('SADD', 'vehiculos', ARGV[3])
return 1`;

// Inserta un lote completo en una sola sentencia; ON CONFLICT descarta eventos ya guardados (idempotencia)
function insertarLote(pool: Pool, filas: any[][]) {
  const marcadores = filas.map((_, i) => {
    const b = i * 10;
    return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6},$${b + 7},$${b + 8},$${b + 9},to_timestamp($${b + 10}/1000.0))`;
  });
  return pool.query(
    `INSERT INTO evento_ocupacion (id, id_vehiculo, ruta, id_estacion, nivel, ocupacion_pct, pasajeros, lat, lon, ts)
     VALUES ${marcadores.join(',')} ON CONFLICT (id, ts) DO NOTHING`,
    filas.flat(),
  );
}

@Injectable()
export class ConsumidorOcupacion implements OnModuleInit, OnModuleDestroy {
  kafka = new Kafka({
    clientId: `ocupacion-${hostname()}`,
    brokers: process.env.KAFKA_BROKERS.split(','),
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  });
  consumidor: Consumer = this.kafka.consumer({ groupId: 'servicio-ocupacion', sessionTimeout: 10000, heartbeatInterval: 2000, maxWaitTimeInMs: 50 });
  productor: Producer = this.kafka.producer({ allowAutoTopicCreation: false });
  redis = new Redis({ host: process.env.REDIS_HOST, password: process.env.REDIS_PASSWORD, maxRetriesPerRequest: 2 });
  pool = new Pool({
    host: process.env.POSTGRES_HOST, database: process.env.POSTGRES_DB,
    user: process.env.POSTGRES_USER, password: process.env.POSTGRES_PASSWORD,
    max: 10, connectionTimeoutMillis: 2000, query_timeout: 3000,
  });

  // Circuit breaker hacia PostgreSQL: si falla más del 50 %, deja de intentar por 10 s
  // y los eventos se guardan en una cola de Redis para escribirse cuando la base vuelva.
  guardarHistorico = new CircuitBreaker((filas: any[][]) => insertarLote(this.pool, filas), {
    timeout: 3000, errorThresholdPercentage: 50, resetTimeout: 10000, volumeThreshold: 5,
  });
  temporizador: NodeJS.Timeout;

  async onModuleInit() {
    this.guardarHistorico.on('open', () => (metricas.circuito_postgres = 'ABIERTO'));
    this.guardarHistorico.on('halfOpen', () => (metricas.circuito_postgres = 'SEMIABIERTO'));
    this.guardarHistorico.on('close', () => (metricas.circuito_postgres = 'CERRADO'));
    this.pool.on('error', (e) => console.warn('PostgreSQL:', e.message));

    await this.productor.connect();
    await this.consumidor.connect();
    this.consumidor.on(this.consumidor.events.GROUP_JOIN, (e) => {
      metricas.particiones_asignadas = e.payload.memberAssignment['ocupacion.eventos'] || [];
      console.log(`Particiones asignadas a ${hostname()}:`, metricas.particiones_asignadas);
    });
    await this.consumidor.subscribe({ topic: 'ocupacion.eventos', fromBeginning: false });

    // Cada partición se procesa en paralelo y en lotes; dentro de una partición el orden se conserva
    await this.consumidor.run({
      partitionsConsumedConcurrently: 3,
      eachBatch: async ({ batch, heartbeat }) => {
        const estados = [];
        const filas = [];
        const actualizacion = this.redis.pipeline();
        for (const mensaje of batch.messages) {
          const evento = JSON.parse(mensaje.value.toString());
          const nivel = evento.ocupacion_pct < 50 ? 'BAJO' : evento.ocupacion_pct < 80 ? 'MEDIO' : 'ALTO';
          const estado = { ...evento, nivel, particion: batch.partition, instancia_ocupacion: hostname(), ts_procesado: Date.now() };
          actualizacion.eval(SCRIPT_ULTIMO_GANA, 1, `vehiculo:${evento.id_vehiculo}`, evento.ts_emision, JSON.stringify(estado), evento.id_vehiculo);
          estados.push(estado);
          filas.push([evento.id_evento, evento.id_vehiculo, evento.ruta, evento.id_estacion, nivel,
            evento.ocupacion_pct, evento.pasajeros, evento.lat, evento.lon, evento.ts_emision]);
        }

        // Solo se difunden los estados que Redis aceptó como más recientes
        const resultados = await actualizacion.exec();
        const nuevos = estados.filter((_, i) => resultados[i][1] === 1);
        metricas.descartados_obsoletos += estados.length - nuevos.length;

        // La publicación del nuevo estado y la escritura del histórico se hacen en paralelo
        await Promise.all([
          nuevos.length
            ? this.productor.send({ topic: 'ocupacion.estado', acks: -1, messages: nuevos.map((e) => ({ key: e.id_vehiculo, value: JSON.stringify(e) })) })
            : null,
          this.guardarHistorico.fire(filas).catch(async () => {
            await this.redis.rpush('pendientes:historico', ...filas.map((f) => JSON.stringify(f)));
            metricas.enviados_a_pendientes += filas.length;
          }),
        ]);
        metricas.procesados += batch.messages.length;
        await heartbeat();
      },
    });

    // Recuperación: cada 5 s intenta escribir en PostgreSQL los eventos que quedaron pendientes
    this.temporizador = setInterval(async () => {
      if (this.guardarHistorico.opened) return;
      const lote = await this.redis.lpop('pendientes:historico', 500).catch(() => null);
      if (!lote) return;
      try {
        await this.guardarHistorico.fire(lote.map((texto) => JSON.parse(texto)));
        metricas.recuperados_de_pendientes += lote.length;
      } catch {
        await this.redis.rpush('pendientes:historico', ...lote);
      }
    }, 5000);
  }

  // Al apagarse avisa al grupo de consumidores para que Kafka reasigne sus particiones de inmediato
  async onModuleDestroy() {
    clearInterval(this.temporizador);
    await this.consumidor.disconnect();
    await this.productor.disconnect();
  }
}
