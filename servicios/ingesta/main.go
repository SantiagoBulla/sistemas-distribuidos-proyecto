// Servicio de Ingesta: recibe los eventos de los buses por HTTP y los publica en Kafka.
// Es un servicio sin estado, por eso se pueden ejecutar varias réplicas en paralelo.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type EventoOcupacion struct {
	IdEvento     string  `json:"id_evento"`
	IdVehiculo   string  `json:"id_vehiculo"`
	Ruta         string  `json:"ruta"`
	IdEstacion   string  `json:"id_estacion"`
	Sentido      string  `json:"sentido"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
	OcupacionPct float64 `json:"ocupacion_pct"`
	Pasajeros    int     `json:"pasajeros"`
	Capacidad    int     `json:"capacidad"`
	TsEmision    int64   `json:"ts_emision"`
	Instancia    string  `json:"instancia_ingesta"`
}

type Incidencia struct {
	Accion      string `json:"accion"`
	Id          string `json:"id"`
	IdEstacion  string `json:"id_estacion"`
	Tipo        string `json:"tipo"`
	Descripcion string `json:"descripcion"`
	Origen      string `json:"origen"`
	TsEmision   int64  `json:"ts_emision"`
}

var (
	instancia, _      = os.Hostname()
	eventosAceptados  atomic.Int64
	eventosRechazados atomic.Int64
	erroresKafka      atomic.Int64
)

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	apiKey := os.Getenv("INGESTA_API_KEY")

	// RequireAll: el broker confirma solo cuando todas las réplicas en sincronía tienen el mensaje.
	// Hash por clave: todos los eventos de un mismo bus van a la misma partición y conservan su orden.
	escritorOcupacion := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "ocupacion.eventos",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  5,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}
	escritorIncidencias := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "incidencias.reportadas",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  5,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		responderJSON(w, 200, map[string]any{"servicio": "ingesta", "estado": "activo", "instancia": instancia})
	})

	http.HandleFunc("/metricas", func(w http.ResponseWriter, r *http.Request) {
		responderJSON(w, 200, map[string]any{
			"instancia":          instancia,
			"eventos_aceptados":  eventosAceptados.Load(),
			"eventos_rechazados": eventosRechazados.Load(),
			"errores_kafka":      erroresKafka.Load(),
		})
	})

	http.HandleFunc("/eventos", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			responderJSON(w, 405, map[string]string{"error": "Método no permitido"})
			return
		}
		// Los dispositivos se autentican con una clave compartida (comparación en tiempo constante)
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Api-Key")), []byte(apiKey)) != 1 {
			eventosRechazados.Add(1)
			responderJSON(w, 401, map[string]string{"error": "Clave de dispositivo inválida"})
			return
		}

		var evento EventoOcupacion
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if err := json.NewDecoder(r.Body).Decode(&evento); err != nil {
			eventosRechazados.Add(1)
			responderJSON(w, 400, map[string]string{"error": "JSON inválido"})
			return
		}
		if evento.IdVehiculo == "" || evento.Ruta == "" || evento.OcupacionPct < 0 || evento.OcupacionPct > 100 ||
			evento.Lat < -90 || evento.Lat > 90 || evento.Lon < -180 || evento.Lon > 180 || evento.Capacidad <= 0 {
			eventosRechazados.Add(1)
			responderJSON(w, 400, map[string]string{"error": "Campos obligatorios ausentes o fuera de rango"})
			return
		}
		if evento.IdEvento == "" {
			evento.IdEvento = uuid.NewString()
		}
		if evento.TsEmision == 0 {
			evento.TsEmision = time.Now().UnixMilli()
		}
		evento.Instancia = instancia

		valor, _ := json.Marshal(evento)
		ctx, cancelar := context.WithTimeout(r.Context(), 6*time.Second)
		defer cancelar()
		err := escritorOcupacion.WriteMessages(ctx, kafka.Message{Key: []byte(evento.IdVehiculo), Value: valor})
		if err != nil {
			erroresKafka.Add(1)
			log.Printf("error publicando en Kafka: %v", err)
			responderJSON(w, 503, map[string]string{"error": "Broker no disponible, reintente"})
			return
		}
		eventosAceptados.Add(1)
		responderJSON(w, 202, map[string]string{"id_evento": evento.IdEvento})
	})

	http.HandleFunc("/incidencias", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			responderJSON(w, 405, map[string]string{"error": "Método no permitido"})
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Api-Key")), []byte(apiKey)) != 1 {
			responderJSON(w, 401, map[string]string{"error": "Clave de dispositivo inválida"})
			return
		}
		var incidencia Incidencia
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if err := json.NewDecoder(r.Body).Decode(&incidencia); err != nil || incidencia.IdEstacion == "" || incidencia.Tipo == "" {
			responderJSON(w, 400, map[string]string{"error": "Se requiere id_estacion y tipo"})
			return
		}
		incidencia.Accion = "CREAR"
		incidencia.Id = uuid.NewString()
		incidencia.Origen = "SIMULADOR"
		incidencia.TsEmision = time.Now().UnixMilli()

		valor, _ := json.Marshal(incidencia)
		ctx, cancelar := context.WithTimeout(r.Context(), 6*time.Second)
		defer cancelar()
		if err := escritorIncidencias.WriteMessages(ctx, kafka.Message{Key: []byte(incidencia.IdEstacion), Value: valor}); err != nil {
			erroresKafka.Add(1)
			responderJSON(w, 503, map[string]string{"error": "Broker no disponible, reintente"})
			return
		}
		responderJSON(w, 202, map[string]string{"id": incidencia.Id})
	})

	log.Printf("Ingesta %s escuchando en :8080, brokers=%v", instancia, brokers)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func responderJSON(w http.ResponseWriter, codigo int, cuerpo any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(codigo)
	json.NewEncoder(w).Encode(cuerpo)
}
