package main
import (
	"./worker"
	"sync"
)
func main() {
	//Set up workers
	resourcesmap := make(worker.ServerResourcesMap)
	lCh := make(chan string, 10000)
	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker.Worker(i, lCh, wg, resourcesmap)
	}

	ns := worker.NameServer(lCh)
	worker.ProcessRequests(ns, lCh)

	close(lCh)
	wg.Wait()
}