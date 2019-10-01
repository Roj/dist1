package worker

import (
	"fmt"
	"net"
	"os"
	"strings"
	"math/rand"
	"sync"
	"time"
	"../storage"
)
// Para manejar los recursos que utiliza el analisis de
// un servidor necesitamos un lock. Esto también, en
// la prueba de concepto, permite fijarnos cuándo terminó
// el análisis.
type ServerResources struct {
	nqueued int
	nthreads int
	lock *sync.Mutex
}
type ServerResourcesMap map[string]*ServerResources

const MAXTHREADSANDQUEUED = 3

// Inicia los recursos para un servidor FTP si no estaba ya disponible
// Poscondicion: existe en el mapa la key host, hay al menos un task
// queued.
func initServerResources(host string, dict ServerResourcesMap) {
	if _, ok := dict[host]; !ok {
		dict[host] = &ServerResources{1, 0, &sync.Mutex{}}
	}
}

// Wrap sencillo de write para poder imprimir e ir viendo.
func send(conn net.Conn, s string) {
	//fmt.Printf(">%s", s)
	conn.Write([]byte(s))
}

func registerThread(host string, dict ServerResourcesMap) {
	initServerResources(host, dict)
	hostres := dict[host]
	hostres.lock.Lock()
	fmt.Printf("[THREAD] Registering thread!!")
	hostres.nqueued  = hostres.nqueued  - 1
	hostres.nthreads = hostres.nthreads + 1
	hostres.lock.Unlock()
}

func unregisterThread(host string, dict ServerResourcesMap) {
	hostres := dict[host]
	hostres.lock.Lock()
	hostres.nthreads = hostres.nthreads - 1
	fmt.Printf("[THREAD] Unregistering thread - tasks %d threads %d\n", hostres.nqueued, hostres.nthreads)
	if hostres.nqueued == 0 && hostres.nthreads == 0 {
		storage.FinishJob(host)
	}
	hostres.lock.Unlock()
}
func distributeJobs(queue []string, host string, hostres *ServerResources) []string {
	hostres.lock.Lock()
	for i:=0; i < MAXTHREADSANDQUEUED - hostres.nthreads - hostres.nqueued; i++ {
		if len(queue) > 0 {
			subpath := queue[0]
			queue = queue[1:]
			hostres.nqueued = hostres.nqueued + 1
			alsoProcess(fmt.Sprintf("%s %s\n", host, subpath))
		}
	}
	hostres.lock.Unlock()
	return queue
}
func runAnalysis(host string, path string, idworker int, resourcesmap ServerResourcesMap) {
	registerThread(host, resourcesmap)
	defer unregisterThread(host, resourcesmap)
	hostres := resourcesmap[host]
	port := 9100 + idworker
	conn, connbuf, dataserver, e := setupFTP(host, port)
	if e == timeoutError {
		// Volvemos a agregar a la cola.
		hostres.lock.Lock()
		hostres.nqueued = hostres.nqueued + 1
		alsoProcess(fmt.Sprintf("%s %s\n", host, path))
		hostres.lock.Unlock()
		return
	}

	it := 0
	next_thread_check := 0
	queue := []string{path}
	for len(queue) > 0 {
		cpath := queue[0]
		queue = queue[1:]
		sendFTPCommand(fmt.Sprintf("LIST -AQ %s", cpath), conn, connbuf, port)
		node, new_queue := parseLS(cpath, dataserver)
		storage.UpdateNode(host, node)

		// Skip /proc/ and /sys/ because they don't make sense and can cause special errors
		queue = filterStrlist(append(queue, new_queue...), "/proc/")
		queue = filterStrlist(append(queue, new_queue...), "/sys/")
		//fmt.Printf("Adjusted queue is %s\n", queue)

		// See if we can parallelize
		if it == next_thread_check {
			next_thread_check = it + len(new_queue) // check after cleaning out this level
			queue = distributeJobs(queue, host, hostres)
		}
		it = it + 1
	}

	send(conn, "BYE\r\n")
	conn.Close()
	dataserver.Close()
}
func alsoProcess(msg string) {
	//fmt.Printf("Also process.. -- '%s'", msg)
	conn, err := net.DialTimeout("tcp", ":50000", time.Second)
	defer conn.Close()
	if err != nil {
		fmt.Println(err)

		fmt.Printf("Maybe timed out... trying again")
		alsoProcess(msg)
		return
		//TODO

		os.Exit(1)
	}
	send(conn, msg)
	//fmt.Println("Cerrando alsoprocess")
}

func Worker(i int, linkChan chan string, wg *sync.WaitGroup, resourcesmap ServerResourcesMap) {
	rand.Seed(time.Now().UnixNano()+int64(i)*27)
	defer wg.Done()
	fmt.Println("Started worker ", i)
	for dest := range linkChan {

		parts := strings.Split(dest, " ")
		host := parts[0]
		path := "/"
		if len(parts) == 2 {
			path = parts[1]
		}
		fmt.Printf("[THREAD %d] Recibido trabajo [%s:%s]\n", i, host, path)
		runAnalysis(host, path, i, resourcesmap)
		fmt.Printf("[THREAD %d] Finalizado trabajo\n", i, host, path)
	}
}