package main

import (
	"log"
	"net/http"
)

const (
	ADDRESS = ":1234"
)

func connectionHandler() http.HandlerFunc {
	log.Println("Server started")

	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
		case http.MethodPut:
		case http.MethodDelete:
		}
	}
}

func main() {
	log.Fatal(http.ListenAndServe(ADDRESS, connectionHandler()))
}
