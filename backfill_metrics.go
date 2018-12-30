package main

import (
	"math/rand"
	"kubernetes_autoscaler/config"
	"kubernetes_autoscaler/metrics"
	"time"
)

func main () {

	configs := config.GetConf()
	d, err := metrics.NewMetricsDriver(configs.Mongo)
	if err != nil {
		panic(err)
	}
	defer d.CloseConnection()

	metricsService := &metrics.MetricsService{MetricsDriver: d}
	currentTimestamp := time.Now().Unix()
	lastHourTs := int64((int(currentTimestamp / 3600) -1) * 3600)

	resources, err := metricsService.FetchCurrentResources()
	for _, resource := range resources {
		//backfill 2 weaks with mock data
		for i := 1209600; i >= 0; i -= 3600 {
			err := metricsService.MetricsDriver.InsertHourlyMetric(
				metrics.Metric{ResourceName: resource.Name, Timestamp: lastHourTs - int64(i),
				MemoryValue: generateMemory(), CPUValue: generateCPUvalue(),
			})

			if err != nil {
				panic(err)
			}
		}
	}

}

func generateCPUvalue() float64 {
	return rand.Float64()
}

func generateMemory() float64 {
	secondPart := float64(rand.Intn(100)) / 100

	return float64(rand.Intn(2)) + secondPart
}
