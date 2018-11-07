package digital_ocean

import (
	"context"
	"fmt"

	"github.com/digitalocean/godo"
	"github.com/spf13/cast"
	"golang.org/x/oauth2"

	"kubernetes_autoscaler/config"
)

const (
	ActionCompleted = "completed"
	ActionErrored = "errored"
	ActionInProgress = "in-progress"
)

type DigitalOcean interface {
	DropletList () ([]godo.Droplet, error)
	CreateSlaveDroplet (name string) (godo.Droplet, error)
}


type digitalOcean struct {
	client *godo.Client
}

type Droplet struct {
	ID int
	Name string
	SizeSlug string
	Region string
	IP string
	CreatedAt string
	Image image
	Status string
	Stats dropletStats
}

type image struct {
	ID int
	Slug string
	Name string
}

type dropletStats struct {
	Memory int
	Vcpus int
	Disk int
}

type TokenSource struct {
	AccessToken string
}

func (t *TokenSource) Token() (*oauth2.Token, error) {
	token := &oauth2.Token{
		AccessToken: t.AccessToken,
	}
	return token, nil
}

func NewDigitalOcean() (*digitalOcean, error) {
	conf := config.GetConf()

	tokenSource := &TokenSource{
		AccessToken: cast.ToString(conf.Auth["token"]),
	}
	oauthClient := oauth2.NewClient(context.Background(), tokenSource)
	client := digitalOcean{godo.NewClient(oauthClient)}

	return &client, nil
}

func (digitalOcean *digitalOcean) DropletList() ([]Droplet, error) {
	// create empty context
	ctx := context.TODO()

	// create a list to hold our droplets
	list := []godo.Droplet{}

	// create options. initially, these will be blank
	opt := &godo.ListOptions{}
	for {
		droplets, resp, err := digitalOcean.client.Droplets.List(ctx, opt)
		if err != nil {
			return nil, err
		}

		// append the current page's droplets to our list
		for _, d := range droplets {
			list = append(list, d)
		}

		// if we are at the last page, break out the for loop
		if resp.Links == nil || resp.Links.IsLastPage() {
			break
		}

		page, err := resp.Links.CurrentPage()
		if err != nil {
			return nil, err
		}

		// set the page we want for the next request
		opt.Page = page + 1
	}

	var dropletList []Droplet

	for _, digitalOceanDroplet := range list {
		dropletList = append(dropletList, simplifyDropletFormat(&digitalOceanDroplet))
	}

	return dropletList, nil
}

func (digitalOcean *digitalOcean) CreateSlaveDroplet(name string) (Droplet, error) {
	// create empty context
	ctx := context.TODO()

	currentDroplets, err := digitalOcean.DropletList()
	if err != nil {
		return Droplet{}, err
	}

	for _, droplet := range currentDroplets {
		if droplet.Name == name {
			fmt.Println("Blana?")
			return Droplet{}, fmt.Errorf("A droplet with name: %s already exists", name)
		}
	}

	createRequest := &godo.DropletCreateRequest{
		Name:   name,
		Region: "fra1",
		Size:   "s-1vcpu-1gb",
		Image: godo.DropletCreateImage{
			Slug: "docker-18-04",
		},
	}

	digitalOceanDroplet, _, err := digitalOcean.client.Droplets.Create(ctx, createRequest)

	if err != nil {
		fmt.Printf("Something bad happened: %s\n\n", err)
		return Droplet{}, err
	}

	// here should fetch if the ip address is not ok and is up and running

	return simplifyDropletFormat(digitalOceanDroplet), nil
}

func (digitalOcean *digitalOcean) CheckStatusOfDroplet(ID int) (string, error) {
	ctx := context.TODO()
	droplet, _, err := digitalOcean.client.Droplets.Get(ctx, ID)
	
	if err != nil {
		return "", err
	}
	
	return droplet.Status, nil
}

func (digitalOcean *digitalOcean) ShutdownOnDroplet(ID int) (int, error) {
	ctx := context.TODO()

	action, _, err := digitalOcean.client.DropletActions.Shutdown(ctx, ID)
	if err != nil {
		return 0, err
	}

	return action.ID, nil
}

func (digitalOcean *digitalOcean) PowerOffDroplet(ID int) (int, error) {
	ctx := context.TODO()

	action, _, err := digitalOcean.client.DropletActions.Shutdown(ctx, ID)
	if err != nil {
		return 0, err
	}

	return action.ID, nil
}

func (digitalOcean *digitalOcean) PowerOnDroplet(ID int) (int, error) {
	ctx := context.TODO()

	action, _, err := digitalOcean.client.DropletActions.PowerOn(ctx, ID)
	if err != nil {
		return 0, err
	}

	return action.ID, nil
}

func (digitalOcean *digitalOcean) RebootDroplet(ID int) (int, error) {
	ctx := context.TODO()

	action, _, err := digitalOcean.client.DropletActions.Reboot(ctx, ID)
	if err != nil {
		return 0, err
	}

	return action.ID, nil
}

//func (digitalOcean *digitalOcean) pollingActionStatusUntilIsDone(action *godo.Action) (string, error) {
//	for {
//		time.Sleep(time.Second)
//		actionStatus, err := digitalOcean.checkDropletAction(action.ResourceID, action.ID)
//		if err != nil {
//			return "", err
//		}
//
//		if actionStatus == ActionInProgress {
//			continue
//		}
//
//		return actionStatus, nil
//	}
//}

func (digitalOcean *digitalOcean) CheckDropletAction(dropletID, actionID int) (string, error) {
	ctx := context.TODO()
	action, _, err := digitalOcean.client.DropletActions.Get(ctx, dropletID, actionID)
	if err != nil {
		return "", err
	}

	return action.Status, nil
}

func simplifyDropletFormat(droplet *godo.Droplet) Droplet {
	return Droplet{
		ID: droplet.ID,
		Name: droplet.Name,
		SizeSlug: droplet.SizeSlug,
		IP: droplet.Networks.V4[0].IPAddress,
		CreatedAt: droplet.Created,
		Image: image {
			ID: droplet.Image.ID,
			Slug: droplet.Image.Slug,
			Name: droplet.Image.Name,
		},
		Stats: dropletStats{
			Memory: droplet.Memory,
			Vcpus: droplet.Vcpus,
			Disk: droplet.Disk,
		},
		Status: droplet.Status,
	}
}