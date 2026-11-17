import { Body, Controller, Get, Headers, HttpException, Param, Patch, Post } from '@nestjs/common';
import * as bcrypt from 'bcryptjs';
import * as jwt from 'jsonwebtoken';
import { pool, ROLES, CORREO_VALIDO } from './db';

// Administración de cuentas. Solo el rol ADMINISTRADOR puede usar estas rutas.
@Controller('usuarios')
export class UsuariosController {
  // Defensa en profundidad: aunque el gateway ya validó el token, el servicio lo vuelve a verificar
  exigirAdministrador(autorizacion: string) {
    try {
      const datos: any = jwt.verify((autorizacion || '').replace('Bearer ', ''), process.env.JWT_SECRET);
      if (datos.tipo !== 'acceso' || datos.rol !== 'ADMINISTRADOR') throw new Error();
    } catch {
      throw new HttpException({ error: 'Requiere rol ADMINISTRADOR' }, 403);
    }
  }

  @Get()
  async listar(@Headers('authorization') autorizacion: string) {
    this.exigirAdministrador(autorizacion);
    const { rows } = await pool.query(
      `SELECT u.id, u.correo, r.nombre AS rol, u.activo, u.creado_en
       FROM usuario u JOIN rol r ON r.id = u.id_rol ORDER BY u.creado_en`,
    );
    return rows;
  }

  @Post()
  async crear(@Headers('authorization') autorizacion: string, @Body() cuerpo: any) {
    this.exigirAdministrador(autorizacion);
    const correo = (cuerpo?.correo || '').toLowerCase().trim();
    if (!CORREO_VALIDO.test(correo) || !ROLES.includes(cuerpo?.rol) || (cuerpo?.clave || '').length < 8) {
      throw new HttpException({ error: 'Se requiere correo válido, clave (mín. 8) y rol válido' }, 400);
    }
    const hash = await bcrypt.hash(cuerpo.clave, 12);
    const { rows } = await pool.query(
      `INSERT INTO usuario (correo, hash_clave, id_rol)
       SELECT $1, $2, id FROM rol WHERE nombre = $3
       ON CONFLICT (correo) DO NOTHING RETURNING id, correo`,
      [correo, hash, cuerpo.rol],
    );
    if (!rows[0]) throw new HttpException({ error: 'El correo ya está registrado' }, 409);
    return { ...rows[0], rol: cuerpo.rol, activo: true };
  }

  // Permite cambiar el rol o inhabilitar/habilitar una cuenta
  @Patch(':id')
  async actualizar(@Headers('authorization') autorizacion: string, @Param('id') id: string, @Body() cuerpo: any) {
    this.exigirAdministrador(autorizacion);
    if (cuerpo?.rol !== undefined && !ROLES.includes(cuerpo.rol)) {
      throw new HttpException({ error: 'Rol inválido' }, 400);
    }
    const { rows } = await pool.query(
      `UPDATE usuario SET
         activo = COALESCE($2, activo),
         id_rol = COALESCE((SELECT id FROM rol WHERE nombre = $3), id_rol)
       WHERE id::text = $1 RETURNING id, correo, activo`,
      [id, typeof cuerpo?.activo === 'boolean' ? cuerpo.activo : null, cuerpo?.rol ?? null],
    );
    if (!rows[0]) throw new HttpException({ error: 'Usuario no encontrado' }, 404);
    return rows[0];
  }
}
