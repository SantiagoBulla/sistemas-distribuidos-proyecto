// Simulador de flota: hace el papel de los sensores IoT de 25 buses del corredor NQS.
// Cada bus avanza entre estaciones y reporta su ocupación al servicio de Ingesta.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Estacion struct {
	Id  string
	Lat float64
	Lon float64
}

type Ruta struct {
	Codigo string
	Desde  int
	Hasta  int
}

type Bus struct {
	Id        string
	Ruta      Ruta
	Posicion  float64 // posición continua entre estaciones (índice de 0 a 13)
	Sentido   float64 // +1 hacia el sur, -1 hacia el norte
	Pasajeros int
	Capacidad int
	Afluencia float64 // cada bus tiene una demanda distinta (0.6 a 1.3)
}

// Debe coincidir con los datos semilla de infra/postgres/init.sql
var estaciones = []Estacion{
	{"E01", 4.6470, -74.0790}, {"E02", 4.6400, -74.0830}, {"E03", 4.6330, -74.0870},
	{"E04", 4.6250, -74.0900}, {"E05", 4.6170, -74.0920}, {"E06", 4.6100, -74.0950},
	{"E07", 4.6040, -74.1000}, {"E08", 4.5990, -74.1040}, {"E09", 4.5940, -74.1090},
	{"E10", 4.5890, -74.1140}, {"E11", 4.5850, -74.1190}, {"E12", 4.5810, -74.1240},
	{"E13", 4.5770, -74.1300}, {"E14", 4.5730, -74.1360},
}

var rutas = []Ruta{{"G43", 1, 14}, {"G47", 3, 14}, {"B74", 1, 13}, {"H15", 4, 12}}

var tiposIncidencia = []string{"PROTESTA", "CIERRE", "RIÑA", "HURTO", "EMERGENCIA_MEDICA"}

func main() {
	numBuses, _ := strconv.Atoi(os.Getenv("SIM_BUSES"))
	intervaloMs, _ := strconv.Atoi(os.Getenv("SIM_INTERVALO_MS"))
	probIncidencia, _ := strconv.ParseFloat(os.Getenv("SIM_PROB_INCIDENCIA"), 64)
	urlIngesta := os.Getenv("INGESTA_URL")
	apiKey := os.Getenv("INGESTA_API_KEY")

	// Sin keep-alive: cada petición vuelve a resolver el DNS y se reparte entre las réplicas de Ingesta
	cliente := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}

	buses := make([]*Bus, numBuses)
	for i := 0; i < numBuses; i++ {
		ruta := rutas[i%len(rutas)]
		buses[i] = &Bus{
			Id:        fmt.Sprintf("TM-%03d", i+1),
			Ruta:      ruta,
			Posicion:  float64(ruta.Desde-1) + rand.Float64()*float64(ruta.Hasta-ruta.Desde),
			Sentido:   []float64{1, -1}[i%2],
			Pasajeros: rand.Intn(120),
			Capacidad: 160,
			Afluencia: 0.6 + rand.Float64()*0.7,
		}
	}

	log.Printf("Simulando %d buses cada %d ms hacia %s", numBuses, intervaloMs, urlIngesta)
	var enviados, fallidos int
	var mu sync.Mutex

	for tick := 0; ; tick++ {
		inicio := time.Now()
		// Factor de hora pico: la demanda sube y baja en ciclos de 10 minutos para que haya variedad de niveles
		factorPico := 0.5 + 0.5*math.Sin(float64(tick)*2*math.Pi/600)

		var wg sync.WaitGroup
		for _, bus := range buses {
			moverBus(bus, factorPico)
			indice := int(math.Round(bus.Posicion))
			evento := map[string]any{
				"id_vehiculo":   bus.Id,
				"ruta":          bus.Ruta.Codigo,
				"id_estacion":   estaciones[indice].Id,
				"sentido":       map[float64]string{1: "SUR", -1: "NORTE"}[bus.Sentido],
				"lat":           estaciones[indice].Lat + (rand.Float64()-0.5)*0.0005,
				"lon":           estaciones[indice].Lon + (rand.Float64()-0.5)*0.0005,
				"ocupacion_pct": math.Round(float64(bus.Pasajeros)/float64(bus.Capacidad)*10000) / 100,
				"pasajeros":     bus.Pasajeros,
				"capacidad":     bus.Capacidad,
				"ts_emision":    time.Now().UnixMilli(),
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok := enviarConReintentos(cliente, urlIngesta+"/eventos", apiKey, evento)
				mu.Lock()
				if ok {
					enviados++
				} else {
					fallidos++
				}
				mu.Unlock()
			}()
		}
		wg.Wait()

		if rand.Float64() < probIncidencia*float64(numBuses) {
			incidencia := map[string]any{
				"id_estacion": estaciones[rand.Intn(len(estaciones))].Id,
				"tipo":        tiposIncidencia[rand.Intn(len(tiposIncidencia))],
				"descripcion": "Reporte automático del sensor de estación",
			}
			enviarConReintentos(cliente, urlIngesta+"/incidencias", apiKey, incidencia)
		}

		if tick%30 == 0 {
			log.Printf("tick=%d enviados=%d fallidos=%d factor_pico=%.2f", tick, enviados, fallidos, factorPico)
		}
		time.Sleep(time.Duration(intervaloMs)*time.Millisecond - time.Since(inicio))
	}
}

// Mueve el bus media estación por tick y ajusta los pasajeros que suben y bajan
func moverBus(bus *Bus, factorPico float64) {
	bus.Posicion += 0.5 * bus.Sentido
	limiteNorte, limiteSur := float64(bus.Ruta.Desde-1), float64(bus.Ruta.Hasta-1)
	if bus.Posicion >= limiteSur || bus.Posicion <= limiteNorte {
		bus.Posicion = math.Max(limiteNorte, math.Min(limiteSur, bus.Posicion))
		bus.Sentido *= -1
	}
	// Los pasajeros se acercan poco a poco a un objetivo que depende de la hora pico y del bus
	objetivo := float64(bus.Capacidad) * (0.2 + 0.7*factorPico) * bus.Afluencia
	cambio := (objetivo-float64(bus.Pasajeros))*0.15 + float64(rand.Intn(17)-8)
	bus.Pasajeros = max(0, min(bus.Capacidad, bus.Pasajeros+int(cambio)))
}

// Reintentos con espera exponencial y jitter: 3 intentos (100 ms, 200 ms, 400 ms aprox.)
func enviarConReintentos(cliente *http.Client, url, apiKey string, cuerpo map[string]any) bool {
	datos, _ := json.Marshal(cuerpo)
	for intento := 0; intento < 3; intento++ {
		peticion, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(datos))
		peticion.Header.Set("Content-Type", "application/json")
		peticion.Header.Set("X-Api-Key", apiKey)
		respuesta, err := cliente.Do(peticion)
		if err == nil {
			respuesta.Body.Close()
			if respuesta.StatusCode < 500 {
				return respuesta.StatusCode < 300
			}
		}
		espera := time.Duration(100*(1<<intento)) * time.Millisecond
		time.Sleep(espera/2 + time.Duration(rand.Int63n(int64(espera/2))))
	}
	return false
}
