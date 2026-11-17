#!/usr/bin/env bash
# Pruebas de tolerancia a fallos: detiene componentes con el sistema en marcha y mide cómo responde.
# Uso (desde la raíz del repositorio, con el sistema arriba): bash pruebas/tolerancia.sh
set -u
export PYTHONIOENCODING=utf-8
source .env
URL=http://localhost:8080/api
RESULTADO=pruebas/resultados/tolerancia.txt
mkdir -p pruebas/resultados
exec > >(tee "$RESULTADO") 2>&1

login() {
  curl -s -X POST $URL/auth/login -H 'Content-Type: application/json' -d "{\"correo\":\"$1\",\"clave\":\"$2\"}"
}
token_de() { python -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))"; }

# Resume en una línea el estado que reporta el gateway
estado() {
  curl -s $URL/metricas -H "Authorization: Bearer $TOKEN" | python -c "
import sys,json
m=json.load(sys.stdin); g=m['grupos']
print(f\"  brokers={len(m['brokers'])}/3 {m['brokers']} | ev/s={m['eventos_por_segundo']} | p95={m['latencia_ms']['p95']} ms\"
      f\" | consumidores ocupación={g['servicio-ocupacion']['consumidores']} lag={g['servicio-ocupacion']['lag']}\"
      f\" | redis={m['redis']} postgres={m['postgres']} | circuito auth={m['circuito_auth']}\")"
}

TOKEN=$(login "$OPERADOR_CORREO" "$OPERADOR_CLAVE" | token_de)
echo "=================== PRUEBAS DE TOLERANCIA A FALLOS · $(date '+%Y-%m-%d %H:%M:%S') ==================="
echo "Estado inicial:"; estado

echo; echo "---- E1. Caída de un broker de Kafka (kafka2) ----"
docker compose stop kafka2 >/dev/null 2>&1
echo "[$(date +%T)] kafka2 detenido. Esperando 20 s..."; sleep 20; estado
docker compose start kafka2 >/dev/null 2>&1
echo "[$(date +%T)] kafka2 iniciado de nuevo. Esperando 30 s..."; sleep 30; estado
echo "  Eventos del simulador que fallaron tras 3 reintentos:"
docker compose logs simulador --since 60s 2>/dev/null | grep -o "fallidos=[0-9]*" | tail -1 | sed 's/^/    /'

echo; echo "---- E2. Caída de una réplica del servicio de Ocupación ----"
docker stop movia-ocupacion-1 >/dev/null
echo "[$(date +%T)] movia-ocupacion-1 detenido. Kafka reasigna sus particiones. Esperando 15 s..."; sleep 15; estado
docker start movia-ocupacion-1 >/dev/null
echo "[$(date +%T)] movia-ocupacion-1 iniciado. Esperando 20 s..."; sleep 20; estado

echo; echo "---- E3. Caída de una réplica del gateway durante 40 consultas seguidas ----"
OK=0; FALLOS=0
for i in $(seq 1 40); do
  [ $i -eq 10 ] && docker stop movia-gateway-1 >/dev/null && echo "  [consulta 10] movia-gateway-1 detenido"
  CODIGO=$(curl -s -o /dev/null -w '%{http_code}' $URL/ocupacion -H "Authorization: Bearer $TOKEN")
  [ "$CODIGO" = "200" ] && OK=$((OK+1)) || FALLOS=$((FALLOS+1))
  sleep 0.25
done
echo "  Con 2 réplicas (distribuido): $OK respuestas 200, $FALLOS fallidas"
docker stop movia-gateway-2 >/dev/null
OK=0; FALLOS=0
for i in $(seq 1 10); do
  CODIGO=$(curl -s -o /dev/null -w '%{http_code}' $URL/ocupacion -H "Authorization: Bearer $TOKEN")
  [ "$CODIGO" = "200" ] && OK=$((OK+1)) || FALLOS=$((FALLOS+1))
done
echo "  Sin réplicas disponibles (equivalente a un servidor centralizado caído): $OK respuestas 200, $FALLOS fallidas (último código $CODIGO)"
docker start movia-gateway-1 movia-gateway-2 >/dev/null
echo "[$(date +%T)] réplicas del gateway iniciadas. Esperando 20 s..."; sleep 20

echo; echo "---- E4. Caída del servicio de autenticación (circuit breaker) ----"
docker compose stop auth >/dev/null 2>&1
for i in 1 2 3 4 5 6 7 8; do
  INICIO=$(date +%s%N)
  CODIGO=$(curl -s -o /dev/null -w '%{http_code}' -X POST $URL/auth/login -H 'Content-Type: application/json' -d "{\"correo\":\"$CIUDADANO_CORREO\",\"clave\":\"$CIUDADANO_CLAVE\"}")
  echo "  login intento $i -> HTTP $CODIGO en $(( ($(date +%s%N) - INICIO) / 1000000 )) ms"
done
echo "  Consulta de ocupación con un token ya emitido -> HTTP $(curl -s -o /dev/null -w '%{http_code}' $URL/ocupacion -H "Authorization: Bearer $TOKEN") (el JWT se valida sin llamar a auth)"
estado
docker compose start auth >/dev/null 2>&1
echo "[$(date +%T)] auth iniciado. Esperando 15 s a que arranque..."; sleep 15
# Cada intento fallido en estado semiabierto vuelve a abrir el circuito 10 s; se reintenta hasta 6 veces
for i in 1 2 3 4 5 6; do
  CODIGO=$(curl -s -o /dev/null -w '%{http_code}' -X POST $URL/auth/login -H 'Content-Type: application/json' -d "{\"correo\":\"$CIUDADANO_CORREO\",\"clave\":\"$CIUDADANO_CLAVE\"}")
  echo "  [$(date +%T)] login tras la recuperación, intento $i -> HTTP $CODIGO"
  [ "$CODIGO" = "200" ] && break
  sleep 6
done
estado

echo; echo "---- E5. Caída de PostgreSQL (histórico) ----"
FILAS_ANTES=$(docker compose exec -T postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -tAc "SELECT count(*) FROM evento_ocupacion")
docker compose stop postgres >/dev/null 2>&1
echo "[$(date +%T)] postgres detenido. Esperando 25 s..."; sleep 25
echo "  Ocupación en vivo (Redis) sigue respondiendo -> HTTP $(curl -s -o /dev/null -w '%{http_code}' $URL/ocupacion -H "Authorization: Bearer $TOKEN")"
echo "  Eventos en cola de pendientes (Redis): $(docker compose exec -T redis redis-cli -a $REDIS_PASSWORD --no-auth-warning LLEN pendientes:historico)"
estado
docker compose start postgres >/dev/null 2>&1
echo "[$(date +%T)] postgres iniciado. Esperando 40 s a que se vacíe la cola..."; sleep 40
echo "  Eventos en cola de pendientes: $(docker compose exec -T redis redis-cli -a $REDIS_PASSWORD --no-auth-warning LLEN pendientes:historico)"
echo "  Filas de histórico antes: $FILAS_ANTES · después: $(docker compose exec -T postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -tAc "SELECT count(*) FROM evento_ocupacion")"
for i in 1 2; do docker compose exec -T --index $i ocupacion wget -qO- localhost:3000/metricas | sed 's/^/  ocupacion-'$i': /'; echo; done
estado

echo; echo "---- E6. Recuperación automática de un proceso que se cae ----"
docker exec movia-ingesta-1 kill -QUIT 1
echo "[$(date +%T)] proceso de movia-ingesta-1 terminado abruptamente. Esperando 8 s..."; sleep 8
docker inspect -f '  {{.Name}} estado={{.State.Status}} reinicios={{.RestartCount}}' movia-ingesta-1

echo; echo "Estado final:"; estado
echo "=================== FIN · $(date '+%Y-%m-%d %H:%M:%S') ==================="
