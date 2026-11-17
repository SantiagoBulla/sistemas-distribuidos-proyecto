import { Pool } from 'pg';

// Pool de conexiones compartido por los controladores del servicio
export const pool = new Pool({
  host: process.env.POSTGRES_HOST,
  database: process.env.POSTGRES_DB,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  max: 10,
  connectionTimeoutMillis: 3000,
});

export const ROLES = ['CIUDADANO', 'OPERADOR', 'ADMINISTRADOR'];
export const CORREO_VALIDO = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
