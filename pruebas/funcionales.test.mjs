// Pruebas funcionales, de seguridad y de integración. Se ejecutan dentro de la red de Docker
// contra Nginx (misma entrada que usa el navegador) y contra la Ingesta interna.
import { test, describe, before } from 'node:test';
import assert from 'node:assert/strict';
import { io } from 'socket.io-client';

const BASE = 'http://web';
const INGESTA = 'http://ingesta:8080';
const env = process.env;
const tokens = {};

async function llamar(metodo, ruta, { token, cuerpo, base = BASE, cabeceras = {} } = {}) {
  const respuesta = await fetch(`${base}${ruta}`, {
    method: metodo,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...cabeceras },
    body: cuerpo ? JSON.stringify(cuerpo) : undefined,
  });
  return { estado: respuesta.status, datos: await respuesta.json().catch(() => null) };
}

const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

before(async () => {
  for (const [rol, correo, clave] of [
    ['CIUDADANO', env.CIUDADANO_CORREO, env.CIUDADANO_CLAVE],
    ['OPERADOR', env.OPERADOR_CORREO, env.OPERADOR_CLAVE],
    ['ADMINISTRADOR', env.ADMIN_CORREO, env.ADMIN_CLAVE],
  ]) {
    const r = await llamar('POST', '/api/auth/login', { cuerpo: { correo, clave } });
    assert.equal(r.estado, 200, `login de ${rol}`);
    tokens[rol] = r.datos;
  }
});

describe('PF - Autenticación y autorización', () => {
  test('PF-01 login válido entrega JWT de acceso y de refresco con el rol correcto', () => {
    assert.equal(tokens.OPERADOR.rol, 'OPERADOR');
    assert.equal(tokens.OPERADOR.access_token.split('.').length, 3);
    assert.ok(tokens.OPERADOR.refresh_token);
  });

  test('PF-02 login con clave errada responde 401 sin revelar si el correo existe', async () => {
    const r = await llamar('POST', '/api/auth/login', { cuerpo: { correo: env.OPERADOR_CORREO, clave: 'incorrecta' } });
    assert.equal(r.estado, 401);
    assert.equal(r.datos.error, 'Credenciales inválidas');
  });

  test('PF-03 petición sin token o con token alterado responde 401', async () => {
    assert.equal((await llamar('GET', '/api/ocupacion')).estado, 401);
    const alterado = tokens.CIUDADANO.access_token.slice(0, -3) + 'abc';
    assert.equal((await llamar('GET', '/api/ocupacion', { token: alterado })).estado, 401);
  });

  test('PF-04 RBAC: cada rol solo accede a sus rutas (403 en las demás)', async () => {
    assert.equal((await llamar('GET', '/api/usuarios', { token: tokens.CIUDADANO.access_token })).estado, 403);
    assert.equal((await llamar('GET', '/api/metricas', { token: tokens.CIUDADANO.access_token })).estado, 403);
    assert.equal((await llamar('GET', '/api/suscripciones', { token: tokens.OPERADOR.access_token })).estado, 403);
    const admin = await llamar('GET', '/api/usuarios', { token: tokens.ADMINISTRADOR.access_token });
    assert.equal(admin.estado, 200);
    assert.ok(admin.datos.length >= 3);
    assert.ok(admin.datos.every((u) => !('hash_clave' in u)), 'nunca se expone el hash de la clave');
  });

  test('PF-05 el token de refresco renueva el acceso y no sirve como token de acceso', async () => {
    const r = await llamar('POST', '/api/auth/refresh', { cuerpo: { refresh_token: tokens.CIUDADANO.refresh_token } });
    assert.equal(r.estado, 200);
    assert.equal((await llamar('GET', '/api/ocupacion', { token: r.datos.access_token })).estado, 200);
    assert.equal((await llamar('GET', '/api/ocupacion', { token: tokens.CIUDADANO.refresh_token })).estado, 401);
  });

  test('PF-06 registro de ciudadano: crea, rechaza duplicados y claves cortas', async () => {
    const correo = `prueba${Date.now()}@movia.test`;
    assert.equal((await llamar('POST', '/api/auth/registro', { cuerpo: { correo, clave: 'ClaveSegura1' } })).estado, 201);
    assert.equal((await llamar('POST', '/api/auth/registro', { cuerpo: { correo, clave: 'ClaveSegura1' } })).estado, 409);
    assert.equal((await llamar('POST', '/api/auth/registro', { cuerpo: { correo: `x${correo}`, clave: '123' } })).estado, 400);
  });
});

describe('PF - Ocupación en tiempo real', () => {
  test('PF-07 hay 25 buses reportando y su nivel corresponde al porcentaje', async () => {
    const r = await llamar('GET', '/api/ocupacion', { token: tokens.CIUDADANO.access_token });
    assert.equal(r.estado, 200);
    assert.equal(r.datos.length, 25);
    for (const v of r.datos) {
      const esperado = v.ocupacion_pct < 50 ? 'BAJO' : v.ocupacion_pct < 80 ? 'MEDIO' : 'ALTO';
      assert.equal(v.nivel, esperado, `${v.id_vehiculo} con ${v.ocupacion_pct}%`);
    }
  });

  test('PF-08 consulta por ruta: el resumen suma el total de buses de la ruta', async () => {
    const r = await llamar('GET', '/api/ocupacion/ruta/G43', { token: tokens.CIUDADANO.access_token });
    assert.equal(r.estado, 200);
    assert.equal(r.datos.resumen.BAJO + r.datos.resumen.MEDIO + r.datos.resumen.ALTO, r.datos.vehiculos.length);
    assert.ok(r.datos.vehiculos.every((v) => v.ruta === 'G43'));
    assert.equal((await llamar('GET', '/api/ocupacion/ruta/XXX', { token: tokens.CIUDADANO.access_token })).estado, 404);
  });

  test('PF-09 próximo bus por estación: solo buses que se acercan, ordenados por distancia', async () => {
    const r = await llamar('GET', '/api/ocupacion/estacion/E07', { token: tokens.CIUDADANO.access_token });
    assert.equal(r.estado, 200);
    const distancias = r.datos.proximos_hacia_el_sur.map((v) => v.estaciones_de_distancia);
    assert.deepEqual(distancias, [...distancias].sort((a, b) => a - b));
    assert.ok(r.datos.proximos_hacia_el_sur.every((v) => v.sentido === 'SUR'));
  });

  test('PF-10 histórico agregado por minuto desde TimescaleDB y validación de parámetros', async () => {
    const r = await llamar('GET', '/api/historico/ocupacion?minutos=10&intervalo=1', { token: tokens.OPERADOR.access_token });
    assert.equal(r.estado, 200);
    assert.ok(r.datos.length > 0 && Number(r.datos.at(-1).eventos) > 0);
    assert.equal((await llamar('GET', '/api/historico/ocupacion?minutos=-5', { token: tokens.OPERADOR.access_token })).estado, 400);
  });
});

describe('PS - Validación de entradas', () => {
  test('PS-01 incidencia con tipo, estación o descripción inválidos responde 400', async () => {
    const t = tokens.OPERADOR.access_token;
    assert.equal((await llamar('POST', '/api/incidencias', { token: t, cuerpo: { id_estacion: 'E01', tipo: 'DROP TABLE' } })).estado, 400);
    assert.equal((await llamar('POST', '/api/incidencias', { token: t, cuerpo: { id_estacion: "E01' OR 1=1", tipo: 'CIERRE' } })).estado, 400);
    assert.equal((await llamar('POST', '/api/incidencias', { token: t, cuerpo: { id_estacion: 'E01', tipo: 'CIERRE', descripcion: 'x'.repeat(300) } })).estado, 400);
  });

  test('PS-02 la ingesta rechaza dispositivos sin clave y eventos fuera de rango', async () => {
    const evento = { id_vehiculo: 'TM-001', ruta: 'G43', lat: 4.6, lon: -74.1, ocupacion_pct: 50, pasajeros: 80, capacidad: 160 };
    assert.equal((await llamar('POST', '/eventos', { base: INGESTA, cuerpo: evento })).estado, 401);
    const conClave = { base: INGESTA, cabeceras: { 'X-Api-Key': env.INGESTA_API_KEY } };
    assert.equal((await llamar('POST', '/eventos', { ...conClave, cuerpo: { ...evento, ocupacion_pct: 150 } })).estado, 400);
  });
});

describe('PI - Integración entre servicios', () => {
  test('PI-01 flujo completo de incidencia: gateway → Kafka → alertas → Redis/PostgreSQL → WebSocket', async () => {
    const ciudadano = tokens.CIUDADANO.access_token;
    const operador = tokens.OPERADOR.access_token;
    assert.ok([200, 201].includes((await llamar('POST', '/api/suscripciones', { token: ciudadano, cuerpo: { ruta: 'G43' } })).estado));

    // El ciudadano escucha por WebSocket antes de que el operador reporte
    const socket = io(BASE, { path: '/socket.io', transports: ['websocket'], auth: { token: ciudadano } });
    const notificacion = new Promise((resolver) => socket.on('notificacion', resolver));
    await new Promise((resolver) => socket.on('bienvenida', resolver));

    const inicio = Date.now();
    const reporte = await llamar('POST', '/api/incidencias', { token: operador, cuerpo: { id_estacion: 'E07', tipo: 'CIERRE', descripcion: 'Prueba de integración' } });
    assert.equal(reporte.estado, 202);

    const recibida = await Promise.race([notificacion, esperar(5000).then(() => null)]);
    socket.disconnect();
    assert.ok(recibida, 'el ciudadano recibe la notificación push en menos de 5 s');
    assert.equal(recibida.incidencia.id, reporte.datos.id);
    console.log(`      notificación recibida ${Date.now() - inicio} ms después del reporte`);

    const activas = await llamar('GET', '/api/incidencias', { token: ciudadano });
    assert.ok(activas.datos.some((i) => i.id === reporte.datos.id), 'la incidencia aparece como activa (Redis)');
    const notificaciones = await llamar('GET', '/api/notificaciones', { token: ciudadano });
    assert.ok(notificaciones.datos.some((n) => n.mensaje.includes('Comuneros')), 'la notificación quedó persistida');

    assert.equal((await llamar('PATCH', `/api/incidencias/${reporte.datos.id}/cerrar`, { token: operador })).estado, 202);
    await esperar(1500);
    const todas = await llamar('GET', '/api/incidencias?estado=todas', { token: operador });
    assert.equal(todas.datos.find((i) => i.id === reporte.datos.id).estado, 'CERRADA');
  });

  test('PI-02 WebSocket: llegan eventos de ocupación con latencia extremo a extremo < 2 s', async () => {
    const socket = io(BASE, { path: '/socket.io', transports: ['websocket'], auth: { token: tokens.CIUDADANO.access_token } });
    const latencias = [];
    socket.on('ocupacion', (v) => latencias.push(Date.now() - v.ts_emision));
    await esperar(5000);
    socket.disconnect();
    latencias.sort((a, b) => a - b);
    const p95 = latencias[Math.floor(latencias.length * 0.95)];
    console.log(`      ${latencias.length} eventos en 5 s · p50 ${latencias[Math.floor(latencias.length / 2)]} ms · p95 ${p95} ms`);
    assert.ok(latencias.length >= 100, 'al menos 20 eventos por segundo');
    assert.ok(p95 < 2000);
  });

  test('PI-03 WebSocket sin token válido es rechazado', async () => {
    const socket = io(BASE, { path: '/socket.io', transports: ['websocket'], auth: { token: 'falso' }, reconnection: false });
    const rechazo = await Promise.race([
      new Promise((resolver) => socket.on('error_autenticacion', () => resolver(true))),
      esperar(3000).then(() => false),
    ]);
    socket.disconnect();
    assert.ok(rechazo);
  });
});
