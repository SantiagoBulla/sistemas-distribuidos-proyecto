import { useEffect, useRef, useState } from 'react';
import { io } from 'socket.io-client';
import { api, obtenerSesion, guardarSesion } from './api.js';

const TIPOS = ['PROTESTA', 'CIERRE', 'RIÑA', 'HURTO', 'EMERGENCIA_MEDICA', 'OTRO'];
const PESTANAS = {
  CIUDADANO: ['Corredor en vivo', 'Mi estación', 'Incidencias', 'Suscripciones'],
  OPERADOR: ['Corredor en vivo', 'Incidencias', 'Histórico', 'Métricas'],
  ADMINISTRADOR: ['Corredor en vivo', 'Incidencias', 'Histórico', 'Métricas', 'Usuarios'],
};

export default function App() {
  const [sesion, setSesion] = useState(obtenerSesion());
  if (!sesion) return <Login alEntrar={(s) => { guardarSesion(s); setSesion(s); }} />;
  return <Panel sesion={sesion} alSalir={() => { guardarSesion(null); setSesion(null); }} />;
}

function Login({ alEntrar }) {
  const [correo, setCorreo] = useState('');
  const [clave, setClave] = useState('');
  const [error, setError] = useState('');

  async function entrar(e) {
    e.preventDefault();
    setError('');
    try {
      alEntrar(await api('/auth/login', { method: 'POST', body: { correo, clave } }));
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div className="login">
      <form onSubmit={entrar} className="tarjeta">
        <div className="marca">MOVIA <span>· Bogotá</span></div>
        <p className="sutil">Ocupación y alertas en tiempo real · Troncal NQS</p>
        <label>Correo<input value={correo} onChange={(e) => setCorreo(e.target.value)} autoComplete="username" /></label>
        <label>Clave<input type="password" value={clave} onChange={(e) => setClave(e.target.value)} autoComplete="current-password" /></label>
        {error && <div className="error">{error}</div>}
        <button>Ingresar</button>
      </form>
    </div>
  );
}

function Panel({ sesion, alSalir }) {
  const pestanas = PESTANAS[sesion.rol];
  const [pestana, setPestana] = useState(pestanas[0]);
  const [vehiculos, setVehiculos] = useState({});
  const [estaciones, setEstaciones] = useState([]);
  const [rutas, setRutas] = useState([]);
  const [incidencias, setIncidencias] = useState([]);
  const [avisos, setAvisos] = useState([]);
  const [conexion, setConexion] = useState({ estado: 'conectando', instancia: '' });
  const latenciasCliente = useRef([]);

  function avisar(texto, tipo = 'info') {
    const id = Math.random();
    setAvisos((a) => [...a, { id, texto, tipo }]);
    setTimeout(() => setAvisos((a) => a.filter((x) => x.id !== id)), 6000);
  }

  async function cargarIncidencias() {
    setIncidencias(await api('/incidencias').catch(() => []));
  }

  useEffect(() => {
    api('/estaciones').then(setEstaciones).catch(() => {});
    api('/rutas').then(setRutas).catch(() => {});
    api('/ocupacion').then((lista) => setVehiculos(Object.fromEntries(lista.map((v) => [v.id_vehiculo, v])))).catch(() => {});
    cargarIncidencias();

    // WebSocket: el gateway empuja cada cambio de ocupación y cada alerta apenas sale de Kafka
    const socket = io({ path: '/socket.io', transports: ['websocket'], auth: { token: obtenerSesion()?.access_token } });
    socket.on('bienvenida', (d) => setConexion({ estado: 'en línea', instancia: d.instancia_gateway }));
    socket.on('disconnect', () => setConexion((c) => ({ ...c, estado: 'reconectando' })));
    socket.on('connect_error', () => setConexion((c) => ({ ...c, estado: 'reconectando' })));
    socket.on('ocupacion', (v) => {
      latenciasCliente.current.push(Date.now() - v.ts_emision);
      if (latenciasCliente.current.length > 500) latenciasCliente.current.shift();
      setVehiculos((actual) => ({ ...actual, [v.id_vehiculo]: v }));
    });
    socket.on('alerta', (a) => {
      cargarIncidencias();
      if (a.tipo === 'INCIDENCIA_NUEVA') avisar(`Alerta: ${a.mensaje}`, 'alerta');
    });
    socket.on('notificacion', (n) => avisar(`Ruta ${n.ruta}: ${n.mensaje}`, 'notificacion'));
    return () => socket.disconnect();
  }, []);

  const lista = Object.values(vehiculos).sort((a, b) => a.id_vehiculo.localeCompare(b.id_vehiculo));

  return (
    <div className="app">
      <header>
        <div className="marca">MOVIA <span>· Troncal NQS</span></div>
        <nav>
          {pestanas.map((p) => (
            <button key={p} className={p === pestana ? 'activa' : ''} onClick={() => setPestana(p)}>{p}</button>
          ))}
        </nav>
        <div className="usuario">
          <span className={`punto ${conexion.estado === 'en línea' ? 'ok' : 'mal'}`} />
          {conexion.estado} · {conexion.instancia}
          <span className="rol">{sesion.rol}</span>
          <button className="secundario" onClick={alSalir}>Salir</button>
        </div>
      </header>

      <main>
        {pestana === 'Corredor en vivo' && <Corredor vehiculos={lista} estaciones={estaciones} rutas={rutas} incidencias={incidencias} />}
        {pestana === 'Mi estación' && <MiEstacion estaciones={estaciones} vehiculos={vehiculos} />}
        {pestana === 'Incidencias' && <Incidencias sesion={sesion} estaciones={estaciones} incidencias={incidencias} avisar={avisar} recargar={cargarIncidencias} />}
        {pestana === 'Suscripciones' && <Suscripciones rutas={rutas} avisar={avisar} />}
        {pestana === 'Histórico' && <Historico rutas={rutas} />}
        {pestana === 'Métricas' && <Metricas latenciasCliente={latenciasCliente} />}
        {pestana === 'Usuarios' && <Usuarios avisar={avisar} />}
      </main>

      <div className="avisos">
        {avisos.map((a) => <div key={a.id} className={`aviso ${a.tipo}`}>{a.texto}</div>)}
      </div>
    </div>
  );
}

function Corredor({ vehiculos, estaciones, rutas, incidencias }) {
  const [filtro, setFiltro] = useState('');
  const visibles = vehiculos.filter((v) => !filtro || v.ruta === filtro);
  const conteo = { BAJO: 0, MEDIO: 0, ALTO: 0 };
  visibles.forEach((v) => conteo[v.nivel]++);
  const ancho = 1100, margen = 50, paso = (ancho - 2 * margen) / Math.max(1, estaciones.length - 1);
  const xDe = (idEstacion) => margen + estaciones.findIndex((e) => e.id === idEstacion) * paso;
  const apilados = {};

  return (
    <>
      <div className="fila">
        <div className="kpi"><b>{visibles.length}</b><span>buses reportando</span></div>
        <div className="kpi bajo"><b>{conteo.BAJO}</b><span>ocupación baja</span></div>
        <div className="kpi medio"><b>{conteo.MEDIO}</b><span>ocupación media</span></div>
        <div className="kpi alto"><b>{conteo.ALTO}</b><span>ocupación alta</span></div>
        <div className="kpi"><b>{incidencias.length}</b><span>incidencias activas</span></div>
        <select value={filtro} onChange={(e) => setFiltro(e.target.value)}>
          <option value="">Todas las rutas</option>
          {rutas.map((r) => <option key={r.codigo} value={r.codigo}>{r.codigo} · {r.nombre}</option>)}
        </select>
      </div>

      <div className="tarjeta">
        <h3>Corredor NQS (norte → sur)</h3>
        <div className="desplazable">
          <svg viewBox={`0 0 ${ancho} 300`} className="mapa">
            <line x1={margen} x2={ancho - margen} y1="150" y2="150" className="via" />
            <text x={margen} y="22" className="sentido">Buses hacia el SUR ▶</text>
            <text x={margen} y="292" className="sentido">◀ Buses hacia el NORTE</text>
            {estaciones.map((e, i) => {
              const conIncidencia = incidencias.some((inc) => inc.id_estacion === e.id);
              return (
                <g key={e.id}>
                  <circle cx={margen + i * paso} cy="150" r="7" className={conIncidencia ? 'estacion incidencia' : 'estacion'} />
                  {conIncidencia && <text x={margen + i * paso} y="137" textAnchor="middle" className="icono">⚠</text>}
                  <text x={margen + i * paso} y="178" textAnchor="middle" className="nombre-estacion">{e.nombre.replace('NQS - ', '')}</text>
                </g>
              );
            })}
            {visibles.map((v) => {
              const clave = `${v.id_estacion}-${v.sentido}`;
              apilados[clave] = (apilados[clave] || 0) + 1;
              const y = v.sentido === 'SUR' ? 130 - apilados[clave] * 17 : 190 + apilados[clave] * 17;
              return (
                <g key={v.id_vehiculo}>
                  <circle cx={xDe(v.id_estacion)} cy={y} r="7" className={`bus ${v.nivel}`} />
                  <title>{`${v.id_vehiculo} · ${v.ruta} · ${v.ocupacion_pct}%`}</title>
                </g>
              );
            })}
          </svg>
        </div>
      </div>

      <div className="tarjeta">
        <h3>Estado por bus</h3>
        <div className="desplazable">
          <table>
            <thead><tr><th>Bus</th><th>Ruta</th><th>Estación</th><th>Sentido</th><th>Ocupación</th><th>Nivel</th><th>Latencia</th><th>Partición / instancia</th></tr></thead>
            <tbody>
              {visibles.map((v) => (
                <tr key={v.id_vehiculo}>
                  <td>{v.id_vehiculo}</td>
                  <td>{v.ruta}</td>
                  <td>{estaciones.find((e) => e.id === v.id_estacion)?.nombre}</td>
                  <td>{v.sentido}</td>
                  <td><div className="barra"><div className={v.nivel} style={{ width: `${v.ocupacion_pct}%` }} /></div>{v.ocupacion_pct}% ({v.pasajeros})</td>
                  <td><span className={`etiqueta ${v.nivel}`}>{v.nivel}</span></td>
                  <td>{v.latencia_backend_ms ?? '—'} ms</td>
                  <td className="sutil">p{v.particion} · {v.instancia_ocupacion?.slice(0, 6)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function MiEstacion({ estaciones, vehiculos }) {
  const [id, setId] = useState('');
  const [datos, setDatos] = useState(null);

  useEffect(() => {
    if (!id) return;
    api(`/ocupacion/estacion/${id}`).then(setDatos).catch(() => setDatos(null));
  }, [id, vehiculos]);

  const columna = (titulo, lista) => (
    <div className="tarjeta">
      <h3>{titulo}</h3>
      {lista.length === 0 && <p className="sutil">No hay buses acercándose.</p>}
      {lista.map((v, i) => (
        <div key={v.id_vehiculo} className={`proximo ${i === 0 ? 'primero' : ''}`}>
          <span className={`etiqueta ${v.nivel}`}>{v.nivel}</span>
          <b>{v.ruta}</b> · {v.id_vehiculo}
          <span className="sutil"> a {v.estaciones_de_distancia} estaciones · {v.ocupacion_pct}% ocupado</span>
        </div>
      ))}
    </div>
  );

  return (
    <>
      <div className="fila">
        <select value={id} onChange={(e) => setId(e.target.value)}>
          <option value="">Seleccione su estación</option>
          {estaciones.map((e) => <option key={e.id} value={e.id}>{e.nombre} ({e.zona})</option>)}
        </select>
      </div>
      {datos && (
        <div className="dos-columnas">
          {columna(`Próximos buses hacia el sur · ${datos.estacion.nombre}`, datos.proximos_hacia_el_sur)}
          {columna(`Próximos buses hacia el norte · ${datos.estacion.nombre}`, datos.proximos_hacia_el_norte)}
        </div>
      )}
    </>
  );
}

function Incidencias({ sesion, estaciones, incidencias, avisar, recargar }) {
  const [forma, setForma] = useState({ id_estacion: '', tipo: 'PROTESTA', descripcion: '' });
  const puedeReportar = sesion.rol !== 'ADMINISTRADOR';
  const puedeCerrar = sesion.rol !== 'CIUDADANO';

  async function reportar(e) {
    e.preventDefault();
    try {
      const r = await api('/incidencias', { method: 'POST', body: forma });
      avisar(`Reporte enviado (${r.estado})`);
      setForma({ ...forma, descripcion: '' });
    } catch (err) {
      avisar(err.message, 'alerta');
    }
  }

  async function cerrar(id) {
    await api(`/incidencias/${id}/cerrar`, { method: 'PATCH' }).catch((err) => avisar(err.message, 'alerta'));
    setTimeout(recargar, 800);
  }

  return (
    <div className="dos-columnas">
      {puedeReportar && (
        <form className="tarjeta" onSubmit={reportar}>
          <h3>Reportar incidencia</h3>
          <label>Estación
            <select value={forma.id_estacion} onChange={(e) => setForma({ ...forma, id_estacion: e.target.value })} required>
              <option value="">Seleccione…</option>
              {estaciones.map((e) => <option key={e.id} value={e.id}>{e.nombre}</option>)}
            </select>
          </label>
          <label>Tipo
            <select value={forma.tipo} onChange={(e) => setForma({ ...forma, tipo: e.target.value })}>
              {TIPOS.map((t) => <option key={t}>{t}</option>)}
            </select>
          </label>
          <label>Descripción (opcional)
            <textarea maxLength={280} value={forma.descripcion} onChange={(e) => setForma({ ...forma, descripcion: e.target.value })} />
          </label>
          <button>Enviar reporte</button>
          <p className="sutil">El reporte es anónimo: no se guarda quién lo envió.</p>
        </form>
      )}
      <div className="tarjeta">
        <h3>Incidencias activas ({incidencias.length})</h3>
        {incidencias.length === 0 && <p className="sutil">Sin incidencias activas.</p>}
        {incidencias.map((i) => (
          <div key={i.id} className="incidencia">
            <div><b>{i.tipo.replace('_', ' ')}</b> · {i.nombre_estacion} <span className="etiqueta">{i.zona}</span></div>
            <div className="sutil">{new Date(i.ts).toLocaleTimeString()} · origen {i.origen} {i.descripcion ? `· ${i.descripcion}` : ''}</div>
            {puedeCerrar && <button className="secundario" onClick={() => cerrar(i.id)}>Cerrar</button>}
          </div>
        ))}
      </div>
    </div>
  );
}

function Suscripciones({ rutas, avisar }) {
  const [suscripciones, setSuscripciones] = useState([]);
  const [notificaciones, setNotificaciones] = useState([]);
  const [ruta, setRuta] = useState('');

  async function cargar() {
    setSuscripciones(await api('/suscripciones').catch(() => []));
    setNotificaciones(await api('/notificaciones').catch(() => []));
  }
  useEffect(() => {
    cargar();
    const t = setInterval(cargar, 5000);
    return () => clearInterval(t);
  }, []);

  async function suscribir(e) {
    e.preventDefault();
    await api('/suscripciones', { method: 'POST', body: { ruta } }).catch((err) => avisar(err.message, 'alerta'));
    cargar();
  }

  return (
    <div className="dos-columnas">
      <div className="tarjeta">
        <h3>Mis rutas</h3>
        <form onSubmit={suscribir} className="fila">
          <select value={ruta} onChange={(e) => setRuta(e.target.value)} required>
            <option value="">Seleccione una ruta…</option>
            {rutas.map((r) => <option key={r.codigo} value={r.codigo}>{r.codigo} · {r.nombre}</option>)}
          </select>
          <button>Suscribirme</button>
        </form>
        {suscripciones.map((s) => (
          <div key={s.id} className="incidencia">
            <div><b>{s.ruta}</b> · {s.nombre}</div>
            <button className="secundario" onClick={() => api(`/suscripciones/${s.id}`, { method: 'DELETE' }).then(cargar)}>Quitar</button>
          </div>
        ))}
      </div>
      <div className="tarjeta">
        <h3>Notificaciones recibidas</h3>
        {notificaciones.length === 0 && <p className="sutil">Aún no hay notificaciones.</p>}
        {notificaciones.map((n) => (
          <div key={n.id} className="incidencia">
            <div><span className="etiqueta">{n.ruta}</span> {n.mensaje}</div>
            <div className="sutil">{new Date(n.enviada_en).toLocaleString()} · {n.estado_incidencia}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function Historico({ rutas }) {
  const [ruta, setRuta] = useState('');
  const [minutos, setMinutos] = useState(30);
  const [datos, setDatos] = useState([]);

  useEffect(() => {
    const cargar = () => api(`/historico/ocupacion?minutos=${minutos}&intervalo=1${ruta ? `&ruta=${ruta}` : ''}`).then(setDatos).catch(() => setDatos([]));
    cargar();
    const t = setInterval(cargar, 10000);
    return () => clearInterval(t);
  }, [ruta, minutos]);

  const ancho = 1000, alto = 260, m = 40;
  const x = (i) => m + (i * (ancho - 2 * m)) / Math.max(1, datos.length - 1);
  const y = (v) => alto - m - (v / 100) * (alto - 2 * m);
  const linea = datos.map((d, i) => `${i ? 'L' : 'M'}${x(i)},${y(d.ocupacion_promedio)}`).join(' ');

  return (
    <div className="tarjeta">
      <div className="fila">
        <h3>Ocupación promedio por minuto (TimescaleDB)</h3>
        <select value={ruta} onChange={(e) => setRuta(e.target.value)}>
          <option value="">Todas las rutas</option>
          {rutas.map((r) => <option key={r.codigo} value={r.codigo}>{r.codigo}</option>)}
        </select>
        <select value={minutos} onChange={(e) => setMinutos(e.target.value)}>
          {[15, 30, 60, 180].map((v) => <option key={v} value={v}>Últimos {v} min</option>)}
        </select>
      </div>
      <div className="desplazable">
        <svg viewBox={`0 0 ${ancho} ${alto}`} className="grafica">
          {[0, 50, 80, 100].map((v) => (
            <g key={v}>
              <line x1={m} x2={ancho - m} y1={y(v)} y2={y(v)} className="guia" />
              <text x={m - 6} y={y(v) + 4} textAnchor="end">{v}%</text>
            </g>
          ))}
          <path d={linea} className="serie" />
          {datos.map((d, i) => <circle key={i} cx={x(i)} cy={y(d.ocupacion_promedio)} r="3" className="serie-punto"><title>{`${new Date(d.intervalo).toLocaleTimeString()} · ${d.ocupacion_promedio}%`}</title></circle>)}
        </svg>
      </div>
      <p className="sutil">{datos.reduce((s, d) => s + Number(d.eventos), 0)} eventos en el periodo · líneas guía en 50 % (medio) y 80 % (alto)</p>
    </div>
  );
}

function Metricas({ latenciasCliente }) {
  const [m, setM] = useState(null);
  useEffect(() => {
    const cargar = () => api('/metricas').then(setM).catch(() => {});
    cargar();
    const t = setInterval(cargar, 2000);
    return () => clearInterval(t);
  }, []);
  if (!m) return <p className="sutil">Cargando métricas…</p>;

  const cliente = [...latenciasCliente.current].sort((a, b) => a - b);
  const p95Cliente = cliente.length ? cliente[Math.floor(cliente.length * 0.95)] : '—';
  const ocupacion = m.grupos?.['servicio-ocupacion'] || {};
  const alertas = m.grupos?.['servicio-alertas'] || {};
  const tarjeta = (titulo, valor, detalle, estado = '') => (
    <div className={`kpi ${estado}`}><span>{titulo}</span><b>{valor}</b><span className="sutil">{detalle}</span></div>
  );

  return (
    <>
      <div className="rejilla">
        {tarjeta('Latencia p95 (gateway)', `${m.latencia_ms.p95 ?? '—'} ms`, `p50 ${m.latencia_ms.p50} · p99 ${m.latencia_ms.p99} · ${m.latencia_ms.muestras} muestras`, m.latencia_ms.p95 < 2000 ? 'bajo' : 'alto')}
        {tarjeta('Latencia p95 extremo a extremo', `${p95Cliente} ms`, 'medida en este navegador', p95Cliente < 2000 ? 'bajo' : 'alto')}
        {tarjeta('Throughput', `${m.eventos_por_segundo} ev/s`, 'eventos de ocupación (10 s)')}
        {tarjeta('Brokers Kafka activos', `${m.brokers.length} / 3`, m.brokers.join(', ') || 'sin conexión', m.brokers.length === 3 ? 'bajo' : m.brokers.length >= 2 ? 'medio' : 'alto')}
        {tarjeta('Consumidores de ocupación', ocupacion.consumidores ?? '—', `lag: ${ocupacion.lag ?? '—'} mensajes`, ocupacion.consumidores > 0 ? 'bajo' : 'alto')}
        {tarjeta('Consumidores de alertas', alertas.consumidores ?? '—', `lag: ${alertas.lag ?? '—'} mensajes`, alertas.consumidores > 0 ? 'bajo' : 'alto')}
        {tarjeta('Redis (estado en vivo)', m.redis, '', m.redis === 'ACTIVO' ? 'bajo' : 'alto')}
        {tarjeta('PostgreSQL (histórico)', m.postgres, '', m.postgres === 'ACTIVO' ? 'bajo' : 'alto')}
        {tarjeta('Circuito hacia Auth', m.circuito_auth, 'circuit breaker', m.circuito_auth === 'CERRADO' ? 'bajo' : 'alto')}
        {tarjeta('Instancia que respondió', m.instancia_gateway, `${m.clientes_websocket} clientes WebSocket`)}
      </div>
      <p className="sutil">Actualizado: {new Date(m.actualizado).toLocaleTimeString()}</p>
    </>
  );
}

function Usuarios({ avisar }) {
  const [usuarios, setUsuarios] = useState([]);
  const [forma, setForma] = useState({ correo: '', clave: '', rol: 'OPERADOR' });
  const cargar = () => api('/usuarios').then(setUsuarios).catch((e) => avisar(e.message, 'alerta'));
  useEffect(() => { cargar(); }, []);

  async function crear(e) {
    e.preventDefault();
    await api('/usuarios', { method: 'POST', body: forma }).then(() => avisar('Usuario creado')).catch((err) => avisar(err.message, 'alerta'));
    cargar();
  }

  return (
    <div className="dos-columnas">
      <form className="tarjeta" onSubmit={crear}>
        <h3>Crear usuario</h3>
        <label>Correo<input value={forma.correo} onChange={(e) => setForma({ ...forma, correo: e.target.value })} /></label>
        <label>Clave<input type="password" value={forma.clave} onChange={(e) => setForma({ ...forma, clave: e.target.value })} /></label>
        <label>Rol
          <select value={forma.rol} onChange={(e) => setForma({ ...forma, rol: e.target.value })}>
            {['CIUDADANO', 'OPERADOR', 'ADMINISTRADOR'].map((r) => <option key={r}>{r}</option>)}
          </select>
        </label>
        <button>Crear</button>
      </form>
      <div className="tarjeta">
        <h3>Cuentas ({usuarios.length})</h3>
        <table>
          <thead><tr><th>Correo</th><th>Rol</th><th>Estado</th><th></th></tr></thead>
          <tbody>
            {usuarios.map((u) => (
              <tr key={u.id}>
                <td>{u.correo}</td>
                <td><span className="etiqueta">{u.rol}</span></td>
                <td>{u.activo ? 'Activo' : 'Inhabilitado'}</td>
                <td><button className="secundario" onClick={() => api(`/usuarios/${u.id}`, { method: 'PATCH', body: { activo: !u.activo } }).then(cargar)}>{u.activo ? 'Inhabilitar' : 'Habilitar'}</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
