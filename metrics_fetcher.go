package main

import (
	"kubernetes_autoscaler/config"
	"kubernetes_autoscaler/metrics"
	"time"
)



func main() {
	configs := config.GetConf()
	d, err := metrics.NewMetricsDriver(configs.Mongo)
	if err != nil {
		panic(err)
	}
	defer d.CloseConnection()


	metricsService := &metrics.MetricsService{MetricsDriver: d}

	for {
		metricsService.FetchMetrics()

		time.Sleep(30 * time.Second)
	}

}
