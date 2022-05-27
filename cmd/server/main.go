package main

import (
	"github.com/MRSharff/distributed-services-with-go/server"
	"log"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
