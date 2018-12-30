package metrics

type Metric struct {
	MemoryValue  float64 `json:"memory_value" bson:"memory_value"`
	CPUValue     float64 `json:"cpu_value" bson:"cpu_value"`
	Timestamp    int64   `json:"timestamp" bson:"timestamp"`
	ResourceName string  `json:"resource_name" bson:"resource_name"`
}

type Resource struct {
	Name string `json:"name" bson:"name"`
	Type string `json:"type" bson:"type"`
}
