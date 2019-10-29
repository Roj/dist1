package main
import (
	"./worker"
	"sync"
)
func main() {
	ns, err := worker.NameServer()
	if err != nil {
		panic("No se pudo levantar el NameServer")
	}
	defer ns.Close()

	//Set up workers
	var resourcesmap sync.Map
	lCh := make(chan string, 10000)
	defer close(lCh)

	wg := new(sync.WaitGroup)
	defer wg.Wait()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker.Worker(i, lCh, wg, &resourcesmap)
	}


	worker.ProcessRequests(ns, lCh)

}