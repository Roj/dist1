package main

import "./storage"

func main() {
	dblisten, err := storage.StartServer()
	if err != nil {
		panic("ERROR: No se pudo levantar el servidor de storage")
	}
	storage.ProcessRequests(dblisten)
	dblisten.Close()
}