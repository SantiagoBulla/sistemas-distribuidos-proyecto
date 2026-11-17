// Prueba de concurrencia: 100 usuarios virtuales envían eventos al mismo tiempo durante 20 s.
// Al final se compara cuántos eventos aceptó la ingesta con cuántos quedaron en PostgreSQL.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const aceptados = new Counter('eventos_aceptados');

export const options = {
  scenarios: { simultaneos: { executor: 'constant-vus', vus: 100, duration: '20s' } },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000'] },
};

export default function () {
  const bus = `TM-${String(1 + Math.floor(Math.random() * 25)).padStart(3, '0')}`;
  const cuerpo = JSON.stringify({
    id_vehiculo: bus, ruta: 'K6', id_estacion: 'E05', sentido: 'SUR',
    lat: 4.617, lon: -74.092, ocupacion_pct: Math.round(Math.random() * 100), pasajeros: 80, capacidad: 160,
    ts_emision: Date.now() - 60000, // eventos "atrasados": no deben pisar el estado en vivo
  });
  const r = http.post('http://ingesta:8080/eventos', cuerpo, {
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': __ENV.INGESTA_API_KEY },
  });
  if (check(r, { 'evento aceptado (202)': (x) => x.status === 202 })) aceptados.add(1);
}
