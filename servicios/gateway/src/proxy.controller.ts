import { Body, Controller, Get, Headers, Param, Patch, Post, Res } from '@nestjs/common';
import { Response } from 'express';
import CircuitBreaker from 'opossum';
import { Roles } from './comun';

const AUTH_URL = process.env.AUTH_URL;

// Circuit breaker hacia el servicio de autenticación: con 3 o más peticiones y 50 % de fallos
// en una ventana de 30 s se abre durante 10 s y responde 503 de inmediato, sin esperar el tiempo límite.
export const circuitoAuth = new CircuitBreaker(
  async (metodo: string, ruta: string, cuerpo: any, autorizacion?: string) => {
    const respuesta = await fetch(`${AUTH_URL}${ruta}`, {
      method: metodo,
      headers: { 'Content-Type': 'application/json', Authorization: autorizacion || '' },
      body: metodo === 'GET' ? undefined : JSON.stringify(cuerpo || {}),
      signal: AbortSignal.timeout(3000),
    });
    const datos = await respuesta.json().catch(() => ({}));
    if (respuesta.status >= 500) throw new Error(`auth respondió ${respuesta.status}`);
    return { estado: respuesta.status, datos };
  },
  { timeout: 3500, errorThresholdPercentage: 50, resetTimeout: 10000, volumeThreshold: 3, rollingCountTimeout: 30000 },
);
circuitoAuth.fallback(() => ({
  estado: 503,
  datos: { error: 'Servicio de autenticación no disponible. Intente en unos segundos.', circuito: circuitoAuth.opened ? 'ABIERTO' : 'CERRADO' },
}));

@Controller()
export class ProxyController {
  @Post('auth/login')
  async login(@Body() cuerpo: any, @Res() res: Response) {
    const r = await circuitoAuth.fire('POST', '/auth/login', cuerpo);
    res.status(r.estado).json(r.datos);
  }

  @Post('auth/refresh')
  async refrescar(@Body() cuerpo: any, @Res() res: Response) {
    const r = await circuitoAuth.fire('POST', '/auth/refresh', cuerpo);
    res.status(r.estado).json(r.datos);
  }

  @Post('auth/registro')
  async registrar(@Body() cuerpo: any, @Res() res: Response) {
    const r = await circuitoAuth.fire('POST', '/auth/registro', cuerpo);
    res.status(r.estado).json(r.datos);
  }

  @Get('usuarios')
  @Roles('ADMINISTRADOR')
  async listarUsuarios(@Headers('authorization') autorizacion: string, @Res() res: Response) {
    const r = await circuitoAuth.fire('GET', '/usuarios', null, autorizacion);
    res.status(r.estado).json(r.datos);
  }

  @Post('usuarios')
  @Roles('ADMINISTRADOR')
  async crearUsuario(@Headers('authorization') autorizacion: string, @Body() cuerpo: any, @Res() res: Response) {
    const r = await circuitoAuth.fire('POST', '/usuarios', cuerpo, autorizacion);
    res.status(r.estado).json(r.datos);
  }

  @Patch('usuarios/:id')
  @Roles('ADMINISTRADOR')
  async actualizarUsuario(@Headers('authorization') autorizacion: string, @Param('id') id: string, @Body() cuerpo: any, @Res() res: Response) {
    const r = await circuitoAuth.fire('PATCH', `/usuarios/${encodeURIComponent(id)}`, cuerpo, autorizacion);
    res.status(r.estado).json(r.datos);
  }
}
