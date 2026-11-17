import { CanActivate, ExecutionContext, HttpException, Injectable, SetMetadata } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Kafka, logLevel } from 'kafkajs';
import Redis from 'ioredis';
import { Pool } from 'pg';
import * as jwt from 'jsonwebtoken';
import { hostname } from 'os';

// Conexiones compartidas por los controladores y el gateway de tiempo real
export const kafka = new Kafka({
  clientId: `gateway-${hostname()}`,
  brokers: process.env.KAFKA_BROKERS.split(','),
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 300, retries: 8 },
});
export const productor = kafka.producer({ allowAutoTopicCreation: false });
export const redis = new Redis({ host: process.env.REDIS_HOST, password: process.env.REDIS_PASSWORD, maxRetriesPerRequest: 2 });
export const pool = new Pool({
  host: process.env.POSTGRES_HOST, database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER, password: process.env.POSTGRES_PASSWORD,
  max: 10, connectionTimeoutMillis: 2000, query_timeout: 5000,
});
pool.on('error', (e) => console.warn('PostgreSQL:', e.message));

// Métricas en memoria de esta instancia del gateway
export const metricas = {
  instancia: hostname(),
  latencias_ms: [] as number[], // últimas 2000 latencias emisión -> gateway
  recepciones: [] as number[], // marcas de tiempo de eventos recibidos (ventana de 10 s)
  clientes_ws: 0,
};

export const TODOS_LOS_ROLES = ['CIUDADANO', 'OPERADOR', 'ADMINISTRADOR'];
export const Roles = (...roles: string[]) => SetMetadata('roles', roles);

// Valida el JWT y que el rol del usuario esté entre los permitidos por la ruta (RBAC)
@Injectable()
export class GuardiaRoles implements CanActivate {
  constructor(private reflector: Reflector) {}

  canActivate(contexto: ExecutionContext): boolean {
    const roles: string[] = this.reflector.getAllAndOverride('roles', [contexto.getHandler(), contexto.getClass()]);
    if (!roles) return true;
    const peticion = contexto.switchToHttp().getRequest();
    const token = (peticion.headers.authorization || '').replace('Bearer ', '');
    try {
      peticion.usuario = jwt.verify(token, process.env.JWT_SECRET);
    } catch {
      throw new HttpException({ error: 'Token ausente, inválido o expirado' }, 401);
    }
    if (peticion.usuario.tipo !== 'acceso') throw new HttpException({ error: 'Tipo de token incorrecto' }, 401);
    if (!roles.includes(peticion.usuario.rol)) {
      throw new HttpException({ error: `Acceso denegado para el rol ${peticion.usuario.rol}` }, 403);
    }
    return true;
  }
}
