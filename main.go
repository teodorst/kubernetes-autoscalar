package main

import (
	"kubernetes_autoscaler/digital_ocean"
	"fmt"
)

func main () {
	digitalOcean, err := digital_ocean.NewDigitalOcean()

	if err != nil {
		fmt.Errorf("error initiating the client %v", err)
	}

	dropletList, err := digitalOcean.DropletList()
	if err != nil {
		fmt.Errorf("error fetching droplets list %v", err)
	}
	fmt.Println(dropletList)

	newSlaveDroplet, err := digitalOcean.CreateSlaveDroplet("Slave")
	if err != nil {
		fmt.Println(err)
		return
	}

}
