import { OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { OnGatewayConnection, OnGatewayDisconnect, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import * as jwt from 'jsonwebtoken';
import { hostname } from 'os';
import { kafka, metricas } from './comun';

// Canal WebSocket (Socket.IO). Cada réplica del gateway tiene su propio grupo de consumidores,
// así todas reciben todos los eventos de Kafka y los reenvían a sus clientes conectados.
// El grupo es nuevo en cada arranque: tras un reinicio no se reenvían eventos viejos, solo los actuales.
@WebSocketGateway({ path: '/socket.io', transports: ['websocket'] })
export class TiempoReal implements OnGatewayConnection, OnGatewayDisconnect, OnModuleInit, OnModuleDestroy {
  @WebSocketServer() servidor: Server;
  consumidor = kafka.consumer({
    groupId: `gateway-${hostname()}-${Date.now()}`, sessionTimeout: 10000, heartbeatInterval: 2000, maxWaitTimeInMs: 50,
  });

  // El token se valida en el handshake; sin token válido la conexión se cierra
  handleConnection(cliente: Socket) {
    try {
      const usuario: any = jwt.verify(cliente.handshake.auth?.token || '', process.env.JWT_SECRET);
      cliente.join(`rol:${usuario.rol}`);
      cliente.join(`usuario:${usuario.sub}`);
      cliente.emit('bienvenida', { instancia_gateway: hostname(), rol: usuario.rol });
      cliente.data.autenticado = true;
      metricas.clientes_ws++;
    } catch {
      cliente.emit('error_autenticacion', { error: 'Token inválido o expirado' });
      cliente.disconnect(true);
    }
  }

  handleDisconnect(cliente: Socket) {
    if (cliente.data.autenticado) metricas.clientes_ws--;
  }

  async onModuleInit() {
    await this.consumidor.connect();
    await this.consumidor.subscribe({ topics: ['ocupacion.estado', 'alertas.emitidas'], fromBeginning: false });
    await this.consumidor.run({
      partitionsConsumedConcurrently: 3,
      eachMessage: async ({ topic, message }) => {
        const datos = JSON.parse(message.value.toString());
        if (topic === 'ocupacion.estado') {
          const latencia = Date.now() - datos.ts_emision;
          metricas.latencias_ms.push(latencia);
          if (metricas.latencias_ms.length > 2000) metricas.latencias_ms.shift();
          metricas.recepciones.push(Date.now());
          while (metricas.recepciones[0] < Date.now() - 10000) metricas.recepciones.shift();
          this.servidor.emit('ocupacion', { ...datos, latencia_backend_ms: latencia, instancia_gateway: hostname() });
          return;
        }
        // Alertas: aviso general a todos y notificación personal a los ciudadanos suscritos
        this.servidor.emit('alerta', datos);
        for (const destino of datos.destinatarios || []) {
          this.servidor.to(`usuario:${destino.id_usuario}`).emit('notificacion', { mensaje: datos.mensaje, ruta: destino.ruta, incidencia: datos.incidencia });
        }
      },
    });
  }

  async onModuleDestroy() {
    await this.consumidor.disconnect();
  }
}
