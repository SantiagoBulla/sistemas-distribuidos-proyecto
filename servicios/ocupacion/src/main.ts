import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Controller, Get, Module } from '@nestjs/common';
import { ConsumidorOcupacion, metricas } from './consumidor';

@Controller()
class SaludController {
  @Get('health')
  salud() {
    return { servicio: 'ocupacion', estado: 'activo', instancia: metricas.instancia };
  }

  @Get('metricas')
  verMetricas() {
    return metricas;
  }
}

@Module({ controllers: [SaludController], providers: [ConsumidorOcupacion] })
class AppModule {}

async function iniciar() {
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn', 'log'] });
  app.enableShutdownHooks();
  await app.listen(3000);
}
iniciar();
