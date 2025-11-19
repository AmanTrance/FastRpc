package main

import (
	"io"
	"log"
	"net/http"
	"time"

	"github.com/AmanTrance/FastRpc/benchmarks/common"
)

func echoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST is supported", http.StatusMethodNotAllowed)
		return
	}

	if r.ContentLength != common.PayloadSize {
		http.Error(w, "Incorrect payload size", http.StatusBadRequest)
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}

func main() {
	http.HandleFunc("/echo", echoHandler)
	addr := ":50053"
	log.Printf("HTTP server listening on %s", addr)

	server := &http.Server{
		Addr:        addr,
		ReadTimeout: 10 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
