package main

import (
	"fmt"
	"net/http"
	"encoding/json"
	"github.com/spf13/cast"

	"kubernetes_autoscaler/config"
	"kubernetes_autoscaler/metrics"
)


type MetricsServer struct {
	metricsService metrics.MetricsServiceInterface
}

func (metricsServer MetricsServer) getMetricsResource(w http.ResponseWriter, r *http.Request) {
	fmt.Println("GET params were:", r.URL.Query())

	resourceName := r.URL.Query()["resourceName"][0]
	if resourceName == "" {
		http.Error(w, "Invalid resource name", http.StatusBadRequest)
	}

	startTimestamp := cast.ToInt64(r.URL.Query()["startTimestamp"][0])
	if startTimestamp == 0 {
		http.Error(w, "Invalid start timestamp", http.StatusBadRequest)
	}

	endTimestamp := cast.ToInt64(r.URL.Query()["endTimestamp"][0])
	if endTimestamp == 0 {
		http.Error(w, "Invalid end timestamp", http.StatusBadRequest)
	}

	resourceMetrics, err := metricsServer.metricsService.RetrieveMetrics(resourceName, startTimestamp, endTimestamp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if resourceMetrics == nil {
		resourceMetrics = []metrics.Metric{}
	}

	jsonMetrics, err := json.Marshal(map[string]interface{}{"metrics": resourceMetrics})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonMetrics)
}

func main() {
	configs := config.GetConf()
	d, err := metrics.NewMetricsDriver(configs.Mongo)
	if err != nil {
		panic(err)
	}
	defer d.CloseConnection()

	server := MetricsServer{metricsService: &metrics.MetricsService{MetricsDriver: d}}
	http.HandleFunc("/metrics", server.getMetricsResource)

	http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello World! I'm a HTTP server!")
	})

	http.ListenAndServe(":3001", nil)
}
