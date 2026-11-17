# MOVIA · Bogotá

Plataforma distribuida para el monitoreo de ocupación y alertas de seguridad en la troncal NQS de TransMilenio.
Proyecto final de **Sistemas Distribuidos** — Universidad Manuela Beltrán (2026).
Integrantes: Santiago Bulla Pérez y Jeanpaol López Buitrago.

Un simulador en Go hace el papel de los sensores de 25 buses. Los eventos pasan por un servicio de ingesta,
un clúster de Apache Kafka de 3 nodos y microservicios NestJS que calculan el nivel de ocupación, gestionan
incidencias y envían todo en tiempo real a un tablero web (React) mediante WebSocket.

```
simulador ─► ingesta ×2 ─► Kafka (3 brokers, RF=3) ─► ocupacion ×2 ─► Redis / PostgreSQL+TimescaleDB
                                   ▲        │
                         gateway ×2 ◄───────┘ ─► alertas ─► notificaciones
                                   ▲
              navegador ─► Nginx (TLS 1.3, límite de peticiones)
```

## Requisitos

| Recurso | Mínimo recomendado |
|---|---|
| Docker Desktop / Docker Engine | 24 o superior, con Docker Compose v2 |
| Memoria asignada a Docker | 6 GB (el sistema completo usa unos 3,5 GB) |
| CPU | 4 núcleos |
| Disco | 5 GB libres para imágenes y volúmenes |
| Puertos libres en el anfitrión | 8080, 8443 y 8081 |

No hace falta instalar Node.js, Go ni bases de datos: todo se compila y ejecuta dentro de contenedores.

## Instalación y ejecución

```bash
git clone https://github.com/SantiagoBulla/sistemas-distribuidos-proyecto.git
cd sistemas-distribuidos-proyecto
cp .env.example .env          # en PowerShell: Copy-Item .env.example .env
docker compose up -d --build
docker compose ps             # todos los servicios deben quedar "healthy" (≈ 1-2 min)
```

| URL | Descripción |
|---|---|
| http://localhost:8080 | Tablero web |
| https://localhost:8443 | Tablero web por HTTPS (certificado autofirmado: el navegador mostrará una advertencia) |
| http://localhost:8081 | Kafka UI: brokers, topics y grupos de consumidores (solo desde el equipo local) |

Los usuarios de prueba (uno por rol: administrador, operador y ciudadano) y sus claves están en `.env.example`.

Para detener: `docker compose down` (conserva los datos) o `docker compose down -v` (borra volúmenes).

## Variables de configuración (`.env`)

| Variable | Uso |
|---|---|
| `KAFKA_CLUSTER_ID`, `KAFKA_BROKERS` | Identificador del clúster KRaft y lista de brokers |
| `POSTGRES_*` | Base de datos, usuario, clave y host de PostgreSQL |
| `REDIS_HOST`, `REDIS_PASSWORD` | Conexión a Redis |
| `JWT_SECRET`, `JWT_EXPIRA_MIN` | Firma y duración (minutos) del token de acceso |
| `INGESTA_API_KEY` | Clave que deben enviar los dispositivos (cabecera `X-Api-Key`) |
| `ADMIN_*`, `OPERADOR_*`, `CIUDADANO_*` | Usuarios semilla creados al iniciar `auth` |
| `SIM_BUSES`, `SIM_INTERVALO_MS`, `SIM_PROB_INCIDENCIA` | Tamaño de la flota simulada, frecuencia de envío y probabilidad de incidencias |

## Estructura del repositorio

```
docker-compose.yml        Orquestación de los 17 contenedores
infra/                    Esquema SQL (TimescaleDB) y creación de topics de Kafka
servicios/ingesta         Go · recibe eventos HTTP y los publica en Kafka
servicios/simulador       Go · flota simulada de 25 buses
servicios/auth            NestJS · login, JWT, bcrypt y gestión de usuarios
servicios/ocupacion       NestJS · consumidor por lotes, Redis (estado vivo) y PostgreSQL (histórico)
servicios/alertas         NestJS · incidencias, zonas y notificaciones
servicios/gateway         NestJS · API REST, WebSocket, RBAC y circuit breaker
web/                      React + Nginx (proxy inverso, TLS 1.3, límites de peticiones)
pruebas/                  Pruebas funcionales, k6 (concurrencia y rendimiento) y tolerancia a fallos
docs/diagramas            Fuentes PlantUML (UML) y BPMN 2.0 con sus imágenes
docs/capturas             Evidencias de funcionamiento
```

## Pruebas

```bash
# Funcionales, de seguridad e integración (15 casos)
docker compose --profile pruebas run --rm pruebas

# Concurrencia: 100 usuarios virtuales enviando eventos durante 20 s
docker compose --profile pruebas run --rm k6 run /scripts/concurrencia.js

# Rendimiento: hasta 400 eventos/s + 30 usuarios consultando
docker compose --profile pruebas run --rm k6 run /scripts/rendimiento.js

# Tolerancia a fallos (detiene brokers, réplicas, auth y PostgreSQL). Requiere bash.
bash pruebas/tolerancia.sh
```

En Git Bash para Windows, anteponer `MSYS_NO_PATHCONV=1` a los comandos de k6 para que no se altere la ruta `/scripts`.
Los resultados obtenidos están en `pruebas/resultados/`.

## Escalar un servicio

```bash
docker compose up -d --scale ocupacion=3 --no-recreate
```

El topic `ocupacion.eventos` tiene 6 particiones, así que admite hasta 6 consumidores en paralelo.
