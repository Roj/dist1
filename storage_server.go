package main

import "./storage"

func main() {
	dblisten := storage.StartServer()
	storage.ProcessRequests(dblisten)
	dblisten.Close()
}