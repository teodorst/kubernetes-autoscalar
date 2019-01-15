package main

import (
	"kubernetes_autoscaler/metrics"
	"kubernetes_autoscaler/digital_ocean"
	"strings"

	"math"
	"fmt"
	"os/exec"
	"os"
	"strconv"
	"regexp"
	"time"
)


const (
	CpuTreshold = 0.5
	MemTreshold = 0.5
	RetriesNumber = 10
)

func getCurrentResources(ts int64) (map[string]float64, error) {
	currentResources := map[string]float64{
		"totalCpu": float64(0),
		"totalMem": float64(0),
		"usedCpu": float64(0),
		"usedMem": float64(0),
	}

	nodeMetrics, err := metrics.FetchNodesMetrics(ts)
	if err != nil {
		return currentResources, err
	}

	nodeMetrics = metrics.TransformMetrics(nodeMetrics)

	for _, nodeMetric := range nodeMetrics {
		if  strings.Contains(nodeMetric.ResourceName, "worker") {
			currentResources["totalCpu"] += 1.0
			currentResources["totalMem"] += 1.0
		} else if strings.Contains(nodeMetric.ResourceName, "master") {
			currentResources["totalCpu"] += 2.0
			currentResources["totalMem"] += 2.0
		}
		currentResources["usedCpu"] += nodeMetric.CPUValue
		currentResources["usedMem"] += nodeMetric.MemoryValue
	}
	
	currentResources["usedCpu"] = math.Round(currentResources["usedCpu"]*100)/100
	currentResources["usedMem"] = math.Round(currentResources["usedMem"]*100)/100

	return currentResources, nil
}


func prepareNodeAndConnectToTheKubeCluster() error {
	// run initial.yaml
	fmt.Println("Initializing workers")
	cmd := exec.Command("ansible-playbook","-i",
		"kube-cluster/hosts",
		"kube-cluster/initial.yml")
	err := cmd.Run()
	if err != nil {
		os.Stderr.WriteString(err.Error())
	}
	fmt.Println("Initializing workers complete")

	fmt.Println("Installing kube dependencies")
	cmd = exec.Command("ansible-playbook","-i",
		"kube-cluster/hosts",
		"kube-cluster/kube-dependencies.yml")
	err = cmd.Run()
	if err != nil {
		os.Stderr.WriteString(err.Error())
	}
	fmt.Println("Kube dependencies were installed")


	fmt.Println("Joining node to Kubernetes cluster")
	cmd = exec.Command("ansible-playbook","-i",
		"kube-cluster/hosts",
		"kube-cluster/workers.yml")
	err = cmd.Run()
	if err != nil {
		os.Stderr.WriteString(err.Error())
	}
	fmt.Println("All nodes are connected to cluster")

	return nil
}

func findNextWorkerName(digitalOcean digital_ocean.DigitalOcean) (string, error) {
	droplets, err := digitalOcean.DropletList()
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile("[0-9]+")
	var indexes []int
	for _, droplet := range droplets {
		if strings.Contains(droplet.Name, "worker") {
			foundIndexes := re.FindAllString(droplet.Name, 1)
			if len(foundIndexes) > 0 {
				index, err := strconv.ParseInt(foundIndexes[0], 10, 32)
				if err != nil {
					return "", err
				}
				indexes = append(indexes, int(index))
			}

		}
	}

	max := 0
	for _, e := range indexes {
		if e > max {
			max = e
		}
	}
	return fmt.Sprintf("worker%d", max+1), nil
}

func addNewWorkerToHostsFile(name, IP string) error {
	fmt.Println("Appending worker to hosts file")

	f, err := os.OpenFile("kube-cluster/hosts", os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		return err
	}

	n, err := f.WriteString(fmt.Sprintf("%s ansible_host=%s ansible_user=root\n", name, IP))

	if n > 0 {
		fmt.Println("New worker was appended to hosts file successfully")
		return nil
	}

	return fmt.Errorf("new worker was not appended to hosts file")

}



func main() {
	//configs := config.GetConf()
	//d, err := metrics.NewMetricsDriver(configs.Mongo)
	//if err != nil {
	//	panic(err)
	//}
	//defer d.CloseConnection()

	//metricsService := &metrics.MetricsService{MetricsDriver: d}

	digitalOcean, err := digital_ocean.NewDigitalOcean()
	if err != nil {
		fmt.Println("Couldn't instantiate digital ocean client")
	}

	for {
		currentTimestamp := time.Now().Unix()

		currentResources, err := getCurrentResources(currentTimestamp)
		if err != nil {
			return
		}

		fmt.Println(currentResources)
		if currentResources["usedCpu"] > CpuTreshold* currentResources["totalMem"] ||
					currentResources["usedMem"] > MemTreshold* currentResources["totalMem"] {
			err = addNewWorkerToCluster(digitalOcean)
			if err != nil {
				panic(err)
			}
		}
		break
		time.Sleep(5 * time.Minute)
	}
}

func addNewWorkerToCluster(digitalOcean digital_ocean.DigitalOcean) error {
	// Generate new name
	newWorkerName, err := findNextWorkerName(digitalOcean)
	if err != nil {
		return err
	}

	// Spawn droplet
	fmt.Sprintf("Spawning a new worker machine! Name: %s\n", newWorkerName)
	droplet, err := digitalOcean.CreateSlaveDroplet(newWorkerName)
	if err != nil {
		fmt.Println("Couldn't create a new worker! Bye bye!")
		return err
	}

	// Wait for droplet completion
	retries := 0
	status, err := digitalOcean.CheckStatusOfDroplet(droplet.ID)
	if err != nil {
		return err
	}

	for status != "active" || retries > RetriesNumber {
		fmt.Printf("Waiting for spawning process! Current status %s \n", status)
		status, err = digitalOcean.CheckStatusOfDroplet(droplet.ID)
		if err != nil {
			fmt.Println("Couldn't check status code for droplet. Bye bye!")
			return err
		}
		retries ++
		time.Sleep(15 * time.Second)
	}

	if status != "active" {
		return fmt.Errorf("droplet could not be spawned")
	}

	// Get network info
	droplet, err = digitalOcean.GetDroplet(droplet.ID)
	if err != nil {
		fmt.Println("Couldn't fetch network info for droplet. Bye bye!")
		return err
	}
	fmt.Errorf("New worker spawn has completed succesfuly! Ip: %s\n", droplet.IP)
	fmt.Println(droplet)

	err = addNewWorkerToHostsFile(droplet.Name, droplet.IP)
	if err != nil {
		return fmt.Errorf("could not add new worker to hosts file")
	}

	err = prepareNodeAndConnectToTheKubeCluster()
	if err != nil {
		return fmt.Errorf("could not add node to cluster")
	}

	return nil
}