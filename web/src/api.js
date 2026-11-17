// Cliente HTTP del tablero: agrega el token y lo renueva una vez si expiró
let sesion = null;
try {
  sesion = JSON.parse(sessionStorage.getItem('movia_sesion'));
} catch {}

export function obtenerSesion() {
  return sesion;
}

export function guardarSesion(nueva) {
  sesion = nueva;
  try {
    if (nueva) sessionStorage.setItem('movia_sesion', JSON.stringify(nueva));
    else sessionStorage.removeItem('movia_sesion');
  } catch {}
}

export async function api(ruta, opciones = {}, reintentar = true) {
  const respuesta = await fetch(`/api${ruta}`, {
    ...opciones,
    headers: {
      'Content-Type': 'application/json',
      ...(sesion ? { Authorization: `Bearer ${sesion.access_token}` } : {}),
    },
    body: opciones.body ? JSON.stringify(opciones.body) : undefined,
  });
  if (respuesta.status === 401 && reintentar && sesion?.refresh_token) {
    const r = await fetch('/api/auth/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: sesion.refresh_token }),
    });
    if (r.ok) {
      guardarSesion({ ...sesion, access_token: (await r.json()).access_token });
      return api(ruta, opciones, false);
    }
  }
  const datos = await respuesta.json().catch(() => ({}));
  if (!respuesta.ok) throw new Error(datos.error || `Error ${respuesta.status}`);
  return datos;
}
