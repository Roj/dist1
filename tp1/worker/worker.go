package worker

import (
	"fmt"
	"net"
	"strings"
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

const MAXTHREADSANDQUEUED = 3

// Inicia los recursos para un servidor FTP si no estaba ya disponible
// Poscondicion: existe en el mapa la key host, hay al menos un task
// queued.
func initServerResources(host string, dict *sync.Map) {
	if _, ok := dict.Load(host); !ok {
		hostres := &ServerResources{1, 0, &sync.Mutex{}}
		dict.Store(host, hostres)
	}
}

// Wrap sencillo de write para poder imprimir e ir viendo.
func send(conn net.Conn, s string) {
	//fmt.Printf(">%s\n", s)
	buf := []byte(s)
	count := 0
	for count < len(buf) {
		byteSent, err := conn.Write(buf[count:])
		count += byteSent
		if err == nil {
			return
		}
	}
}

func registerThread(host string, dict *sync.Map) {

	initServerResources(host, dict)
	val, _ := dict.Load(host)
	hostres, casted := val.(*ServerResources)
	if !casted {
		panic("No se pudo castear el valor del mapa a un *ServerResources")
	}
	hostres.lock.Lock()
	defer hostres.lock.Unlock()
	fmt.Printf("[THREAD] Registering thread!!")
	hostres.nqueued  = hostres.nqueued  - 1
	hostres.nthreads = hostres.nthreads + 1
}

func unregisterThread(host string, dict *sync.Map) {
	val, _ := dict.Load(host)
	hostres, casted := val.(*ServerResources)
	if !casted {
		panic("No se pudo castear el valor del mapa a un *ServerResources")
	}
	hostres.lock.Lock()
	defer hostres.lock.Unlock()
	hostres.nthreads = hostres.nthreads - 1
	fmt.Printf("[THREAD] Unregistering thread - tasks %d threads %d\n", hostres.nqueued, hostres.nthreads)
	if hostres.nqueued == 0 && hostres.nthreads == 0 {
		storage.FinishJob(host)
	}
}
func distributeJobs(queue []string, host string, hostres *ServerResources) []string {
	hostres.lock.Lock()
	defer hostres.lock.Unlock()
	for i:=0; i < MAXTHREADSANDQUEUED - hostres.nthreads - hostres.nqueued; i++ {
		if len(queue) > 0 {
			subpath := queue[0]
			queue = queue[1:]
			hostres.nqueued = hostres.nqueued + 1
			queued := alsoProcess(fmt.Sprintf("%s %s\n", host, subpath))
			// Si no se puede agregar a la cola general, lo seguimos procesando
			// en este thread.
			if !queued {
				queue = append(queue, subpath)
			}
		}
	}
	return queue
}
func runAnalysis(host string, path string, idworker int, resourcesmap *sync.Map) {
	registerThread(host, resourcesmap)
	defer unregisterThread(host, resourcesmap)
	val, _ := resourcesmap.Load(host)
	hostres, casted := val.(*ServerResources)
	if !casted {
		panic("No se pudo castear el valor del mapa a un *ServerResources")
	}
	port := 9100 + idworker
	conn, connbuf, dataserver, e := setupFTP(host, port)
	if e == timeoutError {
		// Volvemos a agregar a la cola.
		hostres.lock.Lock()
		hostres.nqueued = hostres.nqueued + 1
		requeue := alsoProcess(fmt.Sprintf("%s %s\n", host, path))
		if ! requeue {
			panic("Timeout on FTP server and Timeout on Worker queue")
		}
		hostres.lock.Unlock()
		return
	} else if e == noListener {
		// El problema es el address. Intentamos con otro
		runAnalysis(host, path, idworker + 1000, resourcesmap)
		return
	}
	defer conn.Close()
	defer dataserver.Close()

	it := 0
	next_thread_check := 0
	queue := []string{path}
	fails := 0
	for len(queue) > 0 {
		cpath := queue[0]
		queue = queue[1:]
		err := sendFTPCommand(fmt.Sprintf("LIST -AQ %s", cpath), conn, connbuf, port)
		if err != nil {
			if fails < 10 {
				queue = append(queue, cpath)
				time.Sleep(time.Second)
				continue
			}
			//Si el servidor dejó de responder, volver a encolar
			//este directorio a la cola general y terminar.
			requeue := alsoProcess(fmt.Sprintf("%s %s\n", host, path))
			if ! requeue {
				panic("Timeout on FTP server and Timeout on Worker queue")
			}
			return
		}
		fails = 0
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
}
// Adds a job to this worker server's general queue.
// In case of timeout it tries once more.
// Returns false on error.
func alsoProcess(msg string) bool {
	//fmt.Printf("Also process.. -- '%s'", msg)
	conn, err := net.DialTimeout("tcp", ":50000", time.Second)
	if err != nil {
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			fmt.Printf("Timed out: waiting one second and trying again")
			time.Sleep(time.Second)
			conn, err = net.DialTimeout("tcp", ":50000", time.Second)
			return err != nil
		}
		return false
	}
	defer conn.Close()
	send(conn, msg)
	return true
	//fmt.Println("Cerrando alsoprocess")
}

func Worker(i int, linkChan chan string, wg *sync.WaitGroup, resourcesmap *sync.Map) {
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