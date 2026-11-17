# Comparativo de rendimiento del servicio de Ocupación (k6, rendimiento.js, 400 eventos/s)

| Versión del consumidor | Réplicas | Latencia simulador→gateway p50 | p95 | p99 | Lag al terminar |
|---|---|---|---|---|---|
| v1 mensaje a mensaje (eachMessage) | 2 | 346 ms | 10 256 ms | 17 271 ms | 5 464 |
| v1 mensaje a mensaje (eachMessage) | 4 | 577 ms | 11 696 ms | 18 901 ms | 5 747 |
| v2 por lotes (eachBatch) | 2 | 205 ms | 523 ms | 738 ms | 153 |

Conclusión: con 6 particiones el paralelismo máximo es 6; agregar réplicas no ayuda si cada
mensaje hace sus propias idas y vueltas a Redis, Kafka y PostgreSQL. Procesar por lotes sí.
