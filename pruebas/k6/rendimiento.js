// Prueba de rendimiento: carga creciente sobre la ingesta (hasta 400 eventos/s)
// y, en paralelo, 30 usuarios consultando la ocupación a través del gateway.
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    ingesta: {
      executor: 'ramping-arrival-rate', exec: 'enviarEvento', startRate: 50, timeUnit: '1s',
      preAllocatedVUs: 100, maxVUs: 300,
      stages: [{ target: 200, duration: '20s' }, { target: 400, duration: '20s' }, { target: 400, duration: '20s' }],
    },
    consultas: { executor: 'constant-vus', exec: 'consultar', vus: 30, duration: '60s' },
  },
  thresholds: {
    'http_req_duration{scenario:ingesta}': ['p(95)<500'],
    'http_req_duration{scenario:consultas}': ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

export function setup() {
  const r = http.post('http://gateway:3000/api/auth/login',
    JSON.stringify({ correo: __ENV.OPERADOR_CORREO, clave: __ENV.OPERADOR_CLAVE }),
    { headers: { 'Content-Type': 'application/json' } });
  return { token: r.json('access_token') };
}

export function enviarEvento() {
  const bus = `TM-${String(1 + Math.floor(Math.random() * 25)).padStart(3, '0')}`;
  const r = http.post('http://ingesta:8080/eventos', JSON.stringify({
    id_vehiculo: bus, ruta: 'K6', id_estacion: 'E05', sentido: 'SUR', lat: 4.617, lon: -74.092,
    ocupacion_pct: 50, pasajeros: 80, capacidad: 160, ts_emision: Date.now() - 60000,
  }), { headers: { 'Content-Type': 'application/json', 'X-Api-Key': __ENV.INGESTA_API_KEY } });
  check(r, { 'ingesta 202': (x) => x.status === 202 });
}

export function consultar(datos) {
  const r = http.get('http://gateway:3000/api/ocupacion', { headers: { Authorization: `Bearer ${datos.token}` } });
  check(r, { 'consulta 200 con 25 buses': (x) => x.status === 200 && x.json().length === 25 });
}

// Al terminar pide al gateway sus métricas: latencia emisión → gateway medida durante la carga
export function teardown(datos) {
  const m = http.get('http://gateway:3000/api/metricas', { headers: { Authorization: `Bearer ${datos.token}` } }).json();
  console.log(`Latencia simulador→gateway durante la carga: p50=${m.latencia_ms.p50} ms p95=${m.latencia_ms.p95} ms p99=${m.latencia_ms.p99} ms · lag ocupación=${m.grupos['servicio-ocupacion'].lag}`);
}
