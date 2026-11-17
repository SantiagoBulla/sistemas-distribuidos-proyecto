#!/bin/bash
# Crea los topics del sistema con factor de replicación 3 (tolera la caída de un broker)
set -e
K=/opt/kafka/bin/kafka-topics.sh
B=kafka1:9092,kafka2:9092,kafka3:9092
$K --bootstrap-server $B --create --if-not-exists --topic ocupacion.eventos     --partitions 6 --replication-factor 3
$K --bootstrap-server $B --create --if-not-exists --topic ocupacion.estado      --partitions 3 --replication-factor 3
$K --bootstrap-server $B --create --if-not-exists --topic incidencias.reportadas --partitions 3 --replication-factor 3
$K --bootstrap-server $B --create --if-not-exists --topic alertas.emitidas      --partitions 3 --replication-factor 3
$K --bootstrap-server $B --describe
