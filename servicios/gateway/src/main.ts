import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { ApiController } from './api.controller';
import { ProxyController } from './proxy.controller';
import { TiempoReal } from './tiempo-real';
import { GuardiaRoles, productor } from './comun';

@Module({
  controllers: [ApiController, ProxyController],
  providers: [TiempoReal, { provide: APP_GUARD, useClass: GuardiaRoles }],
})
class AppModule {}

async function iniciar() {
  await productor.connect();
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn', 'log'] });
  app.setGlobalPrefix('api', { exclude: ['health'] });
  app.enableShutdownHooks();
  await app.listen(3000);
  console.log('API Gateway escuchando en el puerto 3000');
}
iniciar();
