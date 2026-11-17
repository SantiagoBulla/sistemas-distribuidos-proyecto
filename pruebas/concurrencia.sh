#!/usr/bin/env bash
# Ejecuta la prueba de concurrencia de k6 y verifica que no se pierdan ni dupliquen eventos.
# Uso (desde la raíz del repositorio, con el sistema arriba): bash pruebas/concurrencia.sh
source .env
export MSYS_NO_PATHCONV=1
SQL="SELECT count(*) AS filas_en_postgres, count(DISTINCT id) AS ids_unicos FROM evento_ocupacion WHERE ruta='K6';"
docker compose exec -T postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "DELETE FROM evento_ocupacion WHERE ruta='K6';" > /dev/null
docker compose --profile pruebas run --rm k6 run --quiet /scripts/concurrencia.js 2>&1 | grep -v "Container movia"
echo "Esperando 30 s a que los consumidores terminen de procesar..."; sleep 30
docker compose exec -T postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "$SQL"
for i in 1 2; do docker compose exec -T --index $i ocupacion wget -qO- localhost:3000/metricas; echo; done
