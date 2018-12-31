package metrics

import (
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"github.com/spf13/cast"
	"time"
)

const (
	BASE_RESOURCES_URL = "http://159.89.110.74:8001/api/v1"
	BASE_METRCIS_URL = "http://159.89.110.74:8001/apis/metrics.k8s.io/v1beta1"
	)

type MetricsServiceInterface interface {
	FetchAndStoreNewMetrics(ts int64) error
	ProcessLastHourMetrics(ts int64) error
	FlushOldMetricsForLastHour(ts int64) error
	FetchMetrics() error
	RetrieveMetrics(resourceName string, startTs, endTs int64) ([]Metric, error)
	FetchCurrentResources() ([]Resource, error)
}

type MetricsService struct {
	MetricsDriver MetricsDriverInterface
}

func (metricsService *MetricsService) FetchCurrentResources() ([]Resource, error) {
	pods, err := fetchCurrentPods()
	if err != nil {
		return []Resource{}, err
	}
	nodes, err := fetchCurrentNodes()
	if err != nil {
		return []Resource{}, err
	}

	result := pods
	result = append(pods, nodes...)

	return result, nil
}


func (metricsService *MetricsService) RetrieveMetrics(resourceName string, startTs, endTs int64) ([]Metric, error) {
	metrics, err := metricsService.MetricsDriver.GetHourlyMetric(resourceName, startTs, endTs)
	if err != nil {
		return []Metric{}, err
	}

	currentTimestamp := time.Now().Unix()
	currentHourTs := int64(currentTimestamp / 3600) * 3600
	if endTs > currentHourTs {
		lastHourMetric, err := metricsService.processMetricsForLastHour([]Resource{{Name: resourceName}}, currentHourTs)
		if err != nil {
			return []Metric{}, err
		}

		metrics = append(metrics, lastHourMetric[0])
	}

	return metrics, nil
}


func (metricsService *MetricsService) FetchMetrics() error {
	currentTimestamp := time.Now().Unix()

	fmt.Println(fmt.Sprintf("Fetching metrics at %d", currentTimestamp))

	err := metricsService.FetchAndStoreNewMetrics(currentTimestamp)
	if err != nil {
		return err
	}

	err = metricsService.ProcessLastHourMetrics(currentTimestamp)
	if err != nil {
		return err
	}

	err = metricsService.FlushOldMetricsForLastHour(currentTimestamp)
	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("Fetching complete at %d", currentTimestamp))

	return nil
}

func (metricsService *MetricsService) FetchAndStoreNewMetrics(ts int64) error {

	nodesMetrics, err := fetchNodesMetrics(ts)
	if err != nil {
		return err
	}

	podMetrics, err := fetchPodsMetrics(ts)
	if err != nil {
		return err
	}

	nodesMetrics = transformMetrics(nodesMetrics)
	podMetrics = transformMetrics(podMetrics)


	//save them
	for _, metric := range nodesMetrics {
		err := metricsService.MetricsDriver.InsertLastHourMetric(metric)
		if err != nil {
			return err
		}
	}

	for _, metric := range podMetrics {
		metricsService.MetricsDriver.InsertLastHourMetric(metric)
		if err != nil {
			return err
		}
	}

	return nil
}

func (metricsService *MetricsService) ProcessLastHourMetrics(ts int64) error {
	// compute last hour
	lastHourTs := (int64(ts / 3600) - 1) * 3600

	resources, err := metricsService.FetchCurrentResources()
	if err != nil {
		return err
	}

	processedMetrics, err := metricsService.processMetricsForLastHour(resources, lastHourTs)
	if err != nil {
		return err
	}

	for _, processedMetric := range processedMetrics {
		err := metricsService.MetricsDriver.InsertHourlyMetric(processedMetric)
		if err != nil {
			return err
		}
	}

	return nil
}


func (metricsService *MetricsService) FlushOldMetricsForLastHour(ts int64) error {
	currentHourTs := int64(ts / 3600) * 3600
	fmt.Println(fmt.Sprintf("Flushing metrics from the past at %d", ts))
	return metricsService.MetricsDriver.FlushMetrics(currentHourTs-1)
}

func fetchCurrentPods () ([]Resource, error) {
	resp, err := http.Get(fmt.Sprintf("%s/pods", BASE_RESOURCES_URL))
	if err != nil {
		return []Resource{}, err
	}

	if resp.StatusCode != 200 {
		fmt.Errorf("invalid pods fetch return code %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)

	if err != nil {
		return []Resource{}, err
	}

	var resources []Resource
	for _, item := range cast.ToSlice(m["items"]) {
		itemMetadata := cast.ToStringMap(cast.ToStringMap(item)["metadata"])
		resources = append(resources,
			Resource{Name: cast.ToString(itemMetadata["name"]), Type: "POD"})
	}

	return resources, nil
}


func fetchCurrentNodes () ([]Resource, error) {
	resp, err := http.Get(fmt.Sprintf("%s/nodes", BASE_RESOURCES_URL))
	if err != nil {
		return []Resource{}, err
	}

	if resp.StatusCode != 200 {
		fmt.Errorf("invalid pods fetch return code %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)

	if err != nil {
		return []Resource{}, nil
	}

	var resources []Resource
	for _, item := range cast.ToSlice(m["items"]) {
		itemMetadata := cast.ToStringMap(cast.ToStringMap(item)["metadata"])
		resources = append(resources,
			Resource{Name: cast.ToString(itemMetadata["name"]), Type: "Node"})
	}

	return resources, nil
}

func fetchPodsMetrics(ts int64) ([]Metric, error) {
	resp, err := http.Get(fmt.Sprintf("%s/pods", BASE_METRCIS_URL))
	if err != nil {
		return []Metric{}, err
	}

	if resp.StatusCode != 200 {
		fmt.Errorf("invalid pods fetch return code %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)

	if err != nil {
		return []Metric{}, nil
	}

	var resources []Metric
	for _, item := range cast.ToSlice(m["items"]) {
		itemMetadata := cast.ToStringMap(cast.ToStringMap(item)["metadata"])

		var totalMemory float64
		var totalCPU float64
		for _, container := range cast.ToSlice(cast.ToStringMap(item)["containers"]) {
			containerUsage := cast.ToStringMap(cast.ToStringMap(container)["usage"])
			cpuUsageString := cast.ToString(containerUsage["cpu"])
			totalCPU += cast.ToFloat64(cpuUsageString[:len(cpuUsageString)-1])
			memoryUsageString := cast.ToString(containerUsage["memory"])
			totalMemory += cast.ToFloat64(memoryUsageString[:len(memoryUsageString)-2])
		}

		resources = append(resources,
			Metric{Timestamp: ts,
				ResourceName: cast.ToString(itemMetadata["name"]),
				MemoryValue: totalMemory, CPUValue: totalCPU})
	}

	return resources, nil
}


func fetchNodesMetrics(ts int64) ([]Metric, error) {
	resp, err := http.Get(fmt.Sprintf("%s/nodes", BASE_METRCIS_URL))
	if err != nil {
		return []Metric{}, err
	}

	if resp.StatusCode != 200 {
		fmt.Errorf("invalid pods fetch return code %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)

	if err != nil {
		return []Metric{}, nil
	}

	var resources []Metric
	for _, item := range cast.ToSlice(m["items"]) {
		itemMetadata := cast.ToStringMap(cast.ToStringMap(item)["metadata"])
		itemUsage := cast.ToStringMap(cast.ToStringMap(item)["usage"])

		cpuUsageString := cast.ToString(itemUsage["cpu"])
		usageCPU := cast.ToFloat64(cpuUsageString[:len(cpuUsageString)-1])

		memoryUsageString := cast.ToString(itemUsage["memory"])
		usageMemory := cast.ToFloat64(memoryUsageString[:len(memoryUsageString)-2])

		resources = append(resources,
			Metric{Timestamp: ts,
				ResourceName: cast.ToString(itemMetadata["name"]),
				MemoryValue: usageMemory, CPUValue: usageCPU})
	}

	return resources, nil
}

func transformMetrics(metrics []Metric) []Metric {
	var transformedMetrics []Metric

	for _, metric := range metrics {
		transformedMetrics = append(transformedMetrics,
			Metric{ResourceName: metric.ResourceName,
			Timestamp: metric.Timestamp, CPUValue: metric.CPUValue / 1000000000,
			MemoryValue: metric.MemoryValue * 1024 / 1000000000})
	}

	return transformedMetrics
}


func (metricsService *MetricsService) processMetricsForLastHour(resources []Resource, lastHourTs int64) ([]Metric, error) {
	var processedMetrics []Metric

	for _, resource := range resources {
		podMetrics, err := metricsService.MetricsDriver.GetLastHourMetrics(resource.Name, lastHourTs, lastHourTs + 3599)
		if err != nil {
			return processedMetrics, err
		}

		if len(podMetrics) == 0 {
			continue
		}
		fmt.Println(fmt.Sprintf("Processing metrics from last hour at %d", lastHourTs))
		metricsNo := float64(len(podMetrics))
		cpuTotal := 0.0
		memTotal := 0.0

		for _, metric := range podMetrics {
			cpuTotal += metric.CPUValue
			memTotal += metric.MemoryValue
		}
		processedMetrics = append(processedMetrics,
			Metric{MemoryValue: memTotal / metricsNo, CPUValue: cpuTotal / metricsNo, Timestamp:lastHourTs, ResourceName: resource.Name,})

	}

	return processedMetrics, nil
}