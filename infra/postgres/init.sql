-- Esquema de MOVIA. Se ejecuta una sola vez al crear el volumen de PostgreSQL.
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE rol (
  id     SERIAL PRIMARY KEY,
  nombre TEXT UNIQUE NOT NULL
);

CREATE TABLE usuario (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  correo     TEXT UNIQUE NOT NULL,
  hash_clave TEXT NOT NULL,
  id_rol     INT NOT NULL REFERENCES rol(id),
  activo     BOOLEAN NOT NULL DEFAULT TRUE,
  creado_en  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE corredor (
  id     TEXT PRIMARY KEY,
  nombre TEXT NOT NULL
);

CREATE TABLE estacion (
  id          TEXT PRIMARY KEY,
  nombre      TEXT NOT NULL,
  lat         DOUBLE PRECISION NOT NULL,
  lon         DOUBLE PRECISION NOT NULL,
  zona        TEXT NOT NULL CHECK (zona IN ('NORTE','CENTRO','SUR')),
  orden       INT NOT NULL,
  id_corredor TEXT NOT NULL REFERENCES corredor(id)
);

CREATE TABLE ruta (
  id          SERIAL PRIMARY KEY,
  codigo      TEXT UNIQUE NOT NULL,
  nombre      TEXT NOT NULL,
  orden_desde INT NOT NULL,
  orden_hasta INT NOT NULL,
  id_corredor TEXT NOT NULL REFERENCES corredor(id)
);

CREATE TABLE vehiculo (
  id        TEXT PRIMARY KEY,
  placa     TEXT UNIQUE NOT NULL,
  capacidad INT NOT NULL DEFAULT 160,
  id_ruta   INT NOT NULL REFERENCES ruta(id)
);

-- Serie de tiempo: una fila por evento de ocupación (hypertable de TimescaleDB)
CREATE TABLE evento_ocupacion (
  id            UUID NOT NULL,
  id_vehiculo   TEXT NOT NULL REFERENCES vehiculo(id),
  ruta          TEXT NOT NULL,
  id_estacion   TEXT REFERENCES estacion(id),
  nivel         TEXT NOT NULL CHECK (nivel IN ('BAJO','MEDIO','ALTO')),
  ocupacion_pct NUMERIC(5,2) NOT NULL,
  pasajeros     INT NOT NULL,
  lat           DOUBLE PRECISION NOT NULL,
  lon           DOUBLE PRECISION NOT NULL,
  ts            TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (id, ts)
);
SELECT create_hypertable('evento_ocupacion', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX ON evento_ocupacion (ruta, ts DESC);

-- Las incidencias no guardan el usuario que reporta (minimización de datos, Ley 1581 de 2012)
CREATE TABLE incidencia (
  id          UUID PRIMARY KEY,
  id_estacion TEXT NOT NULL REFERENCES estacion(id),
  tipo        TEXT NOT NULL CHECK (tipo IN ('PROTESTA','CIERRE','RIÑA','HURTO','EMERGENCIA_MEDICA','OTRO')),
  descripcion TEXT,
  origen      TEXT NOT NULL CHECK (origen IN ('CIUDADANO','OPERADOR','SIMULADOR')),
  estado      TEXT NOT NULL DEFAULT 'ABIERTA' CHECK (estado IN ('ABIERTA','CERRADA')),
  zona        TEXT NOT NULL,
  ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  cerrada_en  TIMESTAMPTZ
);

CREATE TABLE suscripcion (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  id_usuario UUID NOT NULL REFERENCES usuario(id) ON DELETE CASCADE,
  id_ruta    INT NOT NULL REFERENCES ruta(id),
  activa     BOOLEAN NOT NULL DEFAULT TRUE,
  creado_en  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (id_usuario, id_ruta)
);

CREATE TABLE notificacion (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  id_suscripcion UUID NOT NULL REFERENCES suscripcion(id) ON DELETE CASCADE,
  id_incidencia  UUID NOT NULL REFERENCES incidencia(id),
  mensaje        TEXT NOT NULL,
  enviada_en     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (id_suscripcion, id_incidencia)
);

-- ---------------- Datos semilla del corredor simulado ----------------
INSERT INTO rol (nombre) VALUES ('CIUDADANO'), ('OPERADOR'), ('ADMINISTRADOR');

INSERT INTO corredor VALUES ('NQS', 'Troncal NQS');

INSERT INTO estacion (id, nombre, lat, lon, zona, orden, id_corredor) VALUES
  ('E01', 'Campín',              4.6470, -74.0790, 'NORTE',  1, 'NQS'),
  ('E02', 'U. Nacional',         4.6400, -74.0830, 'NORTE',  2, 'NQS'),
  ('E03', 'Av. El Dorado',       4.6330, -74.0870, 'NORTE',  3, 'NQS'),
  ('E04', 'CAD',                 4.6250, -74.0900, 'CENTRO', 4, 'NQS'),
  ('E05', 'Paloquemao',          4.6170, -74.0920, 'CENTRO', 5, 'NQS'),
  ('E06', 'Ricaurte',            4.6100, -74.0950, 'CENTRO', 6, 'NQS'),
  ('E07', 'Comuneros',           4.6040, -74.1000, 'CENTRO', 7, 'NQS'),
  ('E08', 'Santa Isabel',        4.5990, -74.1040, 'CENTRO', 8, 'NQS'),
  ('E09', 'Sena',                4.5940, -74.1090, 'SUR',    9, 'NQS'),
  ('E10', 'NQS - Calle 30 Sur',  4.5890, -74.1140, 'SUR',   10, 'NQS'),
  ('E11', 'NQS - Calle 38A Sur', 4.5850, -74.1190, 'SUR',   11, 'NQS'),
  ('E12', 'General Santander',   4.5810, -74.1240, 'SUR',   12, 'NQS'),
  ('E13', 'Alquería',            4.5770, -74.1300, 'SUR',   13, 'NQS'),
  ('E14', 'Venecia',             4.5730, -74.1360, 'SUR',   14, 'NQS');

INSERT INTO ruta (codigo, nombre, orden_desde, orden_hasta, id_corredor) VALUES
  ('G43', 'Venecia - Campín',              1, 14, 'NQS'),
  ('G47', 'Venecia - Av. El Dorado',       3, 14, 'NQS'),
  ('B74', 'Alquería - Campín',             1, 13, 'NQS'),
  ('H15', 'General Santander - CAD',       4, 12, 'NQS');

-- 25 vehículos repartidos entre las 4 rutas (misma regla que usa el simulador)
INSERT INTO vehiculo (id, placa, id_ruta)
SELECT 'TM-' || lpad(i::text, 3, '0'), 'TMA' || lpad(i::text, 3, '0'), ((i - 1) % 4) + 1
FROM generate_series(1, 25) AS i;
