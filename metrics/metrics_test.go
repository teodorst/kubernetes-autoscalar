package metrics


import (
	"testing"
	"github.com/stretchr/testify/suite"
	"time"

	"kubernetes_autoscaler/config"
	"github.com/stretchr/testify/assert"
)

type MetricsTestsSuite struct {
	suite.Suite
	metricsService MetricsService
}

func (suite *MetricsTestsSuite) TestFetchAndStoreNewMetrics() {
	t := suite.T()

	currentTimestamp := time.Now().Unix()
	nodesMetrics, err := FetchNodesMetrics(currentTimestamp)
	assert.Nil(t, err, "error should be nil")

	podMetrics, err := fetchPodsMetrics(currentTimestamp)
	assert.Nil(t, err, "error should be nil")

	suite.metricsService.FetchAndStoreNewMetrics(currentTimestamp)

	for _, nodeMetric := range nodesMetrics {

		storedMetrics, err := suite.metricsService.MetricsDriver.GetLastHourMetrics(nodeMetric.ResourceName,
			currentTimestamp-1, currentTimestamp)
		assert.Nil(t, err, "error should be nil")
		assert.Equal(t, len(storedMetrics), 1)
		assert.Equal(t, storedMetrics[0].Timestamp, currentTimestamp)
		assert.True(t, storedMetrics[0].CPUValue > 0, "cpu value should be greater than 0")
		assert.True(t, storedMetrics[0].MemoryValue > 0, "cpu value should be greater than 0")

	}

	for _, podMetric := range podMetrics {

		storedMetrics, err := suite.metricsService.MetricsDriver.GetLastHourMetrics(podMetric.ResourceName,
			currentTimestamp-1, currentTimestamp)
		assert.Nil(t, err, "error should be nil")
		assert.Equal(t, len(storedMetrics), 1)
		assert.Equal(t, storedMetrics[0].Timestamp, currentTimestamp)
		assert.True(t, storedMetrics[0].CPUValue > 0, "cpu value should be greater than 0")
		assert.True(t, storedMetrics[0].MemoryValue > 0, "cpu value should be greater than 0")

	}
}


func (suite *MetricsTestsSuite) TestProcessLastHourMetrics() {
	t := suite.T()

	currentTimestamp := time.Now().Unix()

	pods := []Resource{
		{Name: "etcd-master", Type: "POD"},
		{Name: "kube-apiserver-master", Type: "POD"},
	}

	nodes := []Resource{
		{Name: "master", Type: "NODE"},
	}

	for _, pod := range pods {
		for i := 1; i <= 2; i++ {
			err := suite.metricsService.MetricsDriver.InsertLastHourMetric(
				Metric{MemoryValue: float64(i), CPUValue: float64(i), Timestamp: currentTimestamp - 3600 + int64(i),
					ResourceName: pod.Name})
			assert.Nil(t, err)
		}
	}

	for _, node := range nodes {
		err := suite.metricsService.MetricsDriver.InsertLastHourMetric(
			Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: currentTimestamp - 3600,
				ResourceName: node.Name})
		assert.Nil(t, err)
	}

	suite.metricsService.ProcessLastHourMetrics(currentTimestamp)

	lastHourTs := int64((int(currentTimestamp / 3600) - 1) * 3600)
	for _, pod := range pods {
		metrics, err := suite.metricsService.MetricsDriver.GetHourlyMetric(pod.Name, 0, currentTimestamp)
		assert.Nil(t, err, "error should be nil")
		assert.Equal(t, 1, len(metrics))
		assert.Equal(t, lastHourTs, metrics[0].Timestamp)
		assert.Equal(t, float64(1.5), metrics[0].CPUValue)
		assert.Equal(t, float64(1.5), metrics[0].MemoryValue)
	}

	for _, node := range nodes {
		metrics, err := suite.metricsService.MetricsDriver.GetHourlyMetric(node.Name, 0, currentTimestamp)
		assert.Nil(t, err, "error should be nil")
		assert.Equal(t, 1, len(metrics))
		assert.Equal(t, lastHourTs, metrics[0].Timestamp)
		assert.Equal(t, float64(2), metrics[0].CPUValue)
		assert.Equal(t, float64(1), metrics[0].MemoryValue)
	}
}


func (suite *MetricsTestsSuite) TestProcessLastHourMetricsEmptyPreviousHour() {
	t := suite.T()

	currentTimestamp := time.Now().Unix()

	pods := []Resource{
		{Name: "etcd-master", Type: "POD"},
		{Name: "kube-apiserver-master", Type: "POD"},
	}

	nodes := []Resource{
		{Name: "master", Type: "NODE"},
	}

	for _, pod := range pods {
		for i := 1; i <= 2; i++ {
			err := suite.metricsService.MetricsDriver.InsertLastHourMetric(
				Metric{MemoryValue: float64(i), CPUValue: float64(i), Timestamp: currentTimestamp - int64(i),
					ResourceName: pod.Name})
			assert.Nil(t, err)
		}
	}

	for _, node := range nodes {
		err := suite.metricsService.MetricsDriver.InsertLastHourMetric(
			Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: currentTimestamp,
				ResourceName: node.Name})
		assert.Nil(t, err)
	}

	suite.metricsService.ProcessLastHourMetrics(currentTimestamp)

	for _, pod := range pods {
		metrics, err := suite.metricsService.MetricsDriver.GetHourlyMetric(pod.Name, 0, currentTimestamp)
		assert.Nil(t, err, "error should be nil")
		assert.Empty(t, metrics)
	}

	for _, node := range nodes {
		metrics, err := suite.metricsService.MetricsDriver.GetHourlyMetric(node.Name, 0, currentTimestamp)
		assert.Nil(t, err, "error should be nil")
		assert.Empty(t, metrics)
	}
}

func (suite *MetricsTestsSuite) TestFlushOldMetricsForLastHour() {
	t := suite.T()
	currentTimestamp := time.Now().Unix()
	currentHourTs := int64(int(currentTimestamp / 3600) * 3600)
	lastHourTs := currentHourTs - 3600

	err := suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: currentTimestamp - 7200,
			ResourceName: "res1"})
	assert.Nil(t, err)


	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: currentTimestamp - 3600,
			ResourceName: "res1"})
	assert.Nil(t, err)


	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: lastHourTs,
			ResourceName: "res1"})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: currentHourTs,
			ResourceName: "res1"})

	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: currentTimestamp,
			ResourceName: "res1"})
	assert.Nil(t, err)

	err = suite.metricsService.FlushOldMetricsForLastHour(currentTimestamp)
	assert.Nil(t, err)

	metrics, err := suite.metricsService.MetricsDriver.GetLastHourMetrics("res1", 0, currentTimestamp)
	assert.Nil(t, err)

	assert.Equal(t, len(metrics), 2)
	assert.Equal(t, metrics[0].Timestamp, currentHourTs)
	assert.Equal(t, metrics[1].Timestamp, currentTimestamp)
}


func (suite *MetricsTestsSuite) TestFetchMetrics() {
	t := suite.T()
	currentTimestamp := time.Now().Unix()
	lastHourTs := int64((int(currentTimestamp / 3600) - 1) * 3600)

	resourceName := "etcd-master"

	lastHourTimestamps := []int64{lastHourTs, lastHourTs + 122, lastHourTs + 1444,}

	// Insert existing metrics from previous hours
	err := suite.metricsService.MetricsDriver.InsertHourlyMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: lastHourTs-14400,
			ResourceName: resourceName})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertHourlyMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: lastHourTs-3600,
			ResourceName: resourceName})
	assert.Nil(t, err)

	// Insert existing metrics from last hour
	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(1), Timestamp: lastHourTs + 122,
			ResourceName: resourceName})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(1), Timestamp: lastHourTs + 1444,
			ResourceName: resourceName})

	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(10), CPUValue: float64(10), Timestamp: lastHourTs,
			ResourceName: resourceName})


	//run func
	err = suite.metricsService.FetchMetrics()
	assert.Nil(t, err)

	// check hourly metrics
	hourlyMetrics, err := suite.metricsService.MetricsDriver.GetHourlyMetric(resourceName, 0, currentTimestamp)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(hourlyMetrics))
	assert.Equal(t, hourlyMetrics[0].Timestamp, lastHourTs-14400)
	assert.Equal(t, hourlyMetrics[1].Timestamp, lastHourTs-3600)
	assert.Equal(t, hourlyMetrics[2].Timestamp, lastHourTs)

	// check metrics were cleaned after
	lastHourMetrics, err := suite.metricsService.MetricsDriver.GetLastHourMetrics(resourceName, 0, currentTimestamp)
	assert.Nil(t, err)
	for _, metric := range lastHourMetrics {
		for _, invalidTs := range lastHourTimestamps {
			if metric.Timestamp == invalidTs {
				assert.Fail(t, "metrics were not cleaned after process phase")
			}
		}
	}
}

func (suite *MetricsTestsSuite) TestRetrieveMetrics() {
	t := suite.T()

	resourceName := "etcd-master"
	currentTimestamp := time.Now().Unix()
	currentHourTs := int64((int(currentTimestamp / 3600)) * 3600)
	lastHourTs := currentHourTs - 3600

	// Insert existing metrics from previous hours
	err := suite.metricsService.MetricsDriver.InsertHourlyMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: lastHourTs-14400,
			ResourceName: resourceName})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertHourlyMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: lastHourTs-3600,
			ResourceName: resourceName})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertHourlyMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(2), Timestamp: lastHourTs,
			ResourceName: resourceName})
	assert.Nil(t, err)

	// last hour metrics
	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(1), Timestamp: currentHourTs + 20,
			ResourceName: resourceName})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(1), CPUValue: float64(1), Timestamp: currentHourTs + 10,
			ResourceName: resourceName})
	assert.Nil(t, err)

	err = suite.metricsService.MetricsDriver.InsertLastHourMetric(
		Metric{MemoryValue: float64(10), CPUValue: float64(10), Timestamp: currentTimestamp,
			ResourceName: resourceName})
	assert.Nil(t, err)

	metrics, err := suite.metricsService.RetrieveMetrics(resourceName, 0, currentTimestamp)
	assert.Nil(t, err)
  
	assert.Equal(t, 4, len(metrics))
	assert.Equal(t, lastHourTs - 14400, metrics[0].Timestamp)
	assert.Equal(t, lastHourTs - 3600, metrics[1].Timestamp)
	assert.Equal(t, lastHourTs, metrics[2].Timestamp)
	assert.Equal(t, currentHourTs, metrics[3].Timestamp)
}

func (suite *MetricsTestsSuite) AfterTest(suiteName, testName string) {
	FlushDb(suite.metricsService.MetricsDriver)
}


func TestExampleTestSuite(t *testing.T) {
	configs := config.Config{Mongo: map[string]interface{}{"host": "localhost", "port": 27017, "db": "kubetest"}}
	d, err := NewMetricsDriver(configs.Mongo)
	if err != nil {
		panic(err)
	}
	defer d.CloseConnection()

	metricsService := MetricsService{MetricsDriver: d}
	testSuite := new(MetricsTestsSuite)
	testSuite.metricsService = metricsService
	suite.Run(t, testSuite)
}