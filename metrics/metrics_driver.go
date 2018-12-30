package metrics

import (
	"gopkg.in/mgo.v2"

	"fmt"
	"time"
	"gopkg.in/mgo.v2/bson"
	"github.com/spf13/cast"
)

const (
	ResourceCollection = "resources"
	HourlyMetricsCollection = "metrics_hourly"
	LastHourMetricsCollection = "metrics_last_hour"
)

type MetricsDriverInterface interface {
	CloseConnection()
	InsertHourlyMetric(metric Metric) error
	InsertLastHourMetric(metric Metric) error
	InsertResource(resource Resource) error
	GetAllResources() ([]Resource, error)
	GetHourlyMetric(resourceID string, startTimestamp, endTimestamp int64) ([]Metric, error)
	GetLastHourMetrics(resourceID string, startTimestamp, endTimestamp int64) ([]Metric, error)
	FlushMetrics(endTimestamp int64) error

	GetResourcesCollection() *mgo.Collection
	GetHourlyMetricsCollection() *mgo.Collection
	GetLastHourCollection() *mgo.Collection
}

type MetricsDriver struct {
	dbSession *mgo.Session
	databaseName string
}

func NewMetricsDriver (config map[string]interface{}) (MetricsDriverInterface, error) {
	fmt.Println(fmt.Sprintf("mongodb://%s:%d/", config["host"], config["port"]))
	dbSession, err := mgo.DialWithTimeout(fmt.Sprintf("mongodb://%s:%d", config["host"], config["port"]), 10 * time.Second)
	if err != nil {
		return nil, err
	}

	//ensure indexes
	for _, collectionName := range []string{HourlyMetricsCollection, LastHourMetricsCollection} {
		collection := dbSession.DB(cast.ToString(config["db"])).With(dbSession.Copy()).C(collectionName)
		defer closeCollectionConnection(collection)

		collection.EnsureIndex(mgo.Index{
			Key: []string{"resource_name", "timestamp"},
			Unique: true,
		})
	}

	return &MetricsDriver{dbSession: dbSession, databaseName: cast.ToString(config["db"])}, nil
}

func (d *MetricsDriver) GetResourcesCollection() *mgo.Collection {
	return d.dbSession.DB(d.databaseName).With(d.dbSession.Copy()).C(ResourceCollection)
}

func (d *MetricsDriver) GetHourlyMetricsCollection() *mgo.Collection {
	return d.dbSession.DB(d.databaseName).With(d.dbSession.Copy()).C(HourlyMetricsCollection)
}

func (d *MetricsDriver) GetLastHourCollection() *mgo.Collection {
	return d.dbSession.DB(d.databaseName).With(d.dbSession.Copy()).C(LastHourMetricsCollection)
}

func (d *MetricsDriver) InsertHourlyMetric(metric Metric) error {
	collection := d.GetHourlyMetricsCollection()
	defer closeCollectionConnection(collection)

	return collection.Insert(metric)
}

func (d *MetricsDriver) InsertLastHourMetric(metric Metric) error {
	collection := d.GetLastHourCollection()
	defer closeCollectionConnection(collection)

	return collection.Insert(metric)
}

func (d *MetricsDriver) InsertResource(resource Resource) error {
	collection := d.GetResourcesCollection()
	defer closeCollectionConnection(collection)

	return collection.Insert(resource)
}


func (d *MetricsDriver) GetAllResources() ([]Resource, error) {
	collection := d.GetResourcesCollection()
	defer closeCollectionConnection(collection)

	var resources []Resource
	err := collection.Find(nil).All(&resources)

	return resources, err
}


func (d *MetricsDriver) GetHourlyMetric(resourceName string, startTimestamp, endTimestamp int64) ([]Metric, error) {
	collection := d.GetHourlyMetricsCollection()
	defer closeCollectionConnection(collection)

	query := bson.M{"resource_name": resourceName, "timestamp": bson.M{"$gte": startTimestamp, "$lte": endTimestamp}}
	var metrics []Metric
	err := collection.Find(query).All(&metrics)

	return metrics, err
}

func (d *MetricsDriver) GetLastHourMetrics(resourceName string, startTimestamp, endTimestamp int64) ([]Metric, error) {
	collection := d.GetLastHourCollection()
	defer closeCollectionConnection(collection)

	query := bson.M{"resource_name": resourceName, "timestamp": bson.M{"$gte": startTimestamp, "$lte": endTimestamp}}
	var metrics []Metric
	err := collection.Find(query).All(&metrics)

	return metrics, err
}


func (d *MetricsDriver) FlushMetrics(endTimestamp int64) error {
	collection := d.GetLastHourCollection()
	defer closeCollectionConnection(collection)

	query := bson.M{"timestamp": bson.M{"$lte": endTimestamp}}
	_, err := collection.RemoveAll(query)
	return err
}

func (d *MetricsDriver) CloseConnection() {
	d.dbSession.Close()
}

func closeCollectionConnection (collection *mgo.Collection) {
	collection.Database.Session.Close()
}

func FlushDb(driver MetricsDriverInterface) {
	lastHourCol := driver.GetLastHourCollection()
	lastHourCol.RemoveAll(bson.M{})
	defer closeCollectionConnection(lastHourCol)

	hourlyCol := driver.GetHourlyMetricsCollection()
	hourlyCol.RemoveAll(bson.M{})
	defer closeCollectionConnection(hourlyCol)

	resCol := driver.GetResourcesCollection()
	resCol.RemoveAll(bson.M{})
	defer closeCollectionConnection(resCol)
}
