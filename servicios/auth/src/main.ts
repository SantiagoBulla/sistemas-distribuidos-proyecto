import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Module } from '@nestjs/common';
import * as bcrypt from 'bcryptjs';
import { AuthController } from './auth.controller';
import { UsuariosController } from './usuarios.controller';
import { pool } from './db';

@Module({ controllers: [AuthController, UsuariosController] })
class AppModule {}

// Crea un usuario por rol si todavía no existe (credenciales tomadas de las variables de entorno)
async function crearUsuariosSemilla() {
  const semillas = [
    [process.env.ADMIN_CORREO, process.env.ADMIN_CLAVE, 'ADMINISTRADOR'],
    [process.env.OPERADOR_CORREO, process.env.OPERADOR_CLAVE, 'OPERADOR'],
    [process.env.CIUDADANO_CORREO, process.env.CIUDADANO_CLAVE, 'CIUDADANO'],
  ];
  for (const [correo, clave, rol] of semillas) {
    const hash = await bcrypt.hash(clave, 12);
    await pool.query(
      `INSERT INTO usuario (correo, hash_clave, id_rol)
       SELECT $1, $2, id FROM rol WHERE nombre = $3
       ON CONFLICT (correo) DO NOTHING`,
      [correo, hash, rol],
    );
  }
}

async function iniciar() {
  await crearUsuariosSemilla();
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn', 'log'] });
  await app.listen(3000);
  console.log('Servicio de autenticación escuchando en el puerto 3000');
}
iniciar();
