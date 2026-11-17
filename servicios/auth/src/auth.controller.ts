import { Body, Controller, Get, HttpCode, HttpException, Post } from '@nestjs/common';
import * as bcrypt from 'bcryptjs';
import * as jwt from 'jsonwebtoken';
import { pool, CORREO_VALIDO } from './db';

const SECRETO = process.env.JWT_SECRET;
const MINUTOS_ACCESO = Number(process.env.JWT_EXPIRA_MIN || 15);

@Controller()
export class AuthController {
  @Get('health')
  async salud() {
    await pool.query('SELECT 1');
    return { servicio: 'auth', estado: 'activo' };
  }

  // Valida credenciales y entrega un token de acceso corto y uno de refresco más largo
  @Post('auth/login')
  @HttpCode(200)
  async login(@Body() cuerpo: { correo?: string; clave?: string }) {
    if (!cuerpo?.correo || !cuerpo?.clave) {
      throw new HttpException({ error: 'Se requiere correo y clave' }, 400);
    }
    const { rows } = await pool.query(
      `SELECT u.id, u.correo, u.hash_clave, u.activo, r.nombre AS rol
       FROM usuario u JOIN rol r ON r.id = u.id_rol WHERE u.correo = $1`,
      [cuerpo.correo.toLowerCase().trim()],
    );
    const usuario = rows[0];
    // Mismo mensaje para usuario inexistente o clave errada: no revela qué correos existen
    if (!usuario || !usuario.activo || !(await bcrypt.compare(cuerpo.clave, usuario.hash_clave))) {
      throw new HttpException({ error: 'Credenciales inválidas' }, 401);
    }
    const datos = { sub: usuario.id, correo: usuario.correo, rol: usuario.rol };
    return {
      access_token: jwt.sign({ ...datos, tipo: 'acceso' }, SECRETO, { expiresIn: `${MINUTOS_ACCESO}m` }),
      refresh_token: jwt.sign({ ...datos, tipo: 'refresco' }, SECRETO, { expiresIn: '8h' }),
      rol: usuario.rol,
      correo: usuario.correo,
      expira_en_s: MINUTOS_ACCESO * 60,
    };
  }

  @Post('auth/refresh')
  @HttpCode(200)
  async refrescar(@Body() cuerpo: { refresh_token?: string }) {
    let datos: any;
    try {
      datos = jwt.verify(cuerpo?.refresh_token || '', SECRETO);
    } catch {
      throw new HttpException({ error: 'Token de refresco inválido o expirado' }, 401);
    }
    if (datos.tipo !== 'refresco') throw new HttpException({ error: 'Tipo de token incorrecto' }, 401);

    // Se vuelve a consultar el usuario por si fue inhabilitado o cambió de rol
    const { rows } = await pool.query(
      `SELECT u.id, u.correo, u.activo, r.nombre AS rol FROM usuario u JOIN rol r ON r.id = u.id_rol WHERE u.id = $1`,
      [datos.sub],
    );
    if (!rows[0]?.activo) throw new HttpException({ error: 'Usuario inhabilitado' }, 401);
    const nuevo = { sub: rows[0].id, correo: rows[0].correo, rol: rows[0].rol, tipo: 'acceso' };
    return { access_token: jwt.sign(nuevo, SECRETO, { expiresIn: `${MINUTOS_ACCESO}m` }), rol: rows[0].rol };
  }

  // Registro abierto: siempre crea cuentas con rol CIUDADANO
  @Post('auth/registro')
  async registrar(@Body() cuerpo: { correo?: string; clave?: string }) {
    const correo = (cuerpo?.correo || '').toLowerCase().trim();
    if (!CORREO_VALIDO.test(correo)) throw new HttpException({ error: 'Correo inválido' }, 400);
    if (!cuerpo.clave || cuerpo.clave.length < 8) {
      throw new HttpException({ error: 'La clave debe tener al menos 8 caracteres' }, 400);
    }
    const hash = await bcrypt.hash(cuerpo.clave, 12);
    const { rows } = await pool.query(
      `INSERT INTO usuario (correo, hash_clave, id_rol)
       SELECT $1, $2, id FROM rol WHERE nombre = 'CIUDADANO'
       ON CONFLICT (correo) DO NOTHING RETURNING id, correo`,
      [correo, hash],
    );
    if (!rows[0]) throw new HttpException({ error: 'El correo ya está registrado' }, 409);
    return { id: rows[0].id, correo: rows[0].correo, rol: 'CIUDADANO' };
  }
}
