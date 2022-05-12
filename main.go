package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

var clusterEndpoint string
var username string
var password string
var client *redis.ClusterClient

func init() {

	clusterEndpoint = os.Getenv("MEMORYDB_CLUSTER_ENDPOINT")
	if clusterEndpoint == "" {
		log.Fatal("MEMORYDB_CLUSTER_ENDPOINT env var missing")
	}

	username = os.Getenv("MEMORYDB_USERNAME")
	if username == "" {
		log.Fatal("MEMORYDB_USERNAME env var missing")
	}

	password = os.Getenv("MEMORYDB_PASSWORD")
	if password == "" {
		log.Fatal("MEMORYDB_PASSWORD env var missing")
	}

	log.Println("connecting to cluster", clusterEndpoint)

	opts := &redis.ClusterOptions{Username: username, Password: password,
		Addrs:          []string{clusterEndpoint},
		TLSConfig:      &tls.Config{},
		RouteByLatency: true,
	}

	client = redis.NewClusterClient(opts)

	err := client.Ping(context.Background()).Err()
	if err != nil {
		log.Fatalf("failed to connect to memorydb redis. error message - %v", err)
	}

	log.Println("successfully connected to cluster")
}

func main() {

	r := mux.NewRouter()

	r.HandleFunc("/{key}", set).Methods(http.MethodPost)
	r.HandleFunc("/{key}", get).Methods(http.MethodGet)
	r.HandleFunc("/", clusterNodeInfo).Methods(http.MethodGet)

	log.Println("started HTTP server....")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func get(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	key := vars["key"]

	//log.Println("getting value for key -", key)
	slot := client.ClusterKeySlot(context.Background(), key).Val()
	log.Printf("getting value for key %s that belongs to slot %v\n", key, slot)

	val, err := client.Get(context.Background(), key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	log.Println("got value for key", key)

	kv := KV{Key: key, Value: val}
	err = json.NewEncoder(w).Encode(kv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func set(w http.ResponseWriter, req *http.Request) {

	vars := mux.Vars(req)
	key := vars["key"]

	b, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()

	value := string(b)

	//err := json.NewDecoder(req.Body).Decode(&kv)
	if err != nil {
		log.Fatal("failed to decode request body - ", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Printf("setting %s=%s\n", key, value)

	err = client.Set(context.Background(), key, value, 0).Err()
	if err != nil {
		log.Fatalf("failed to set %s=%s\n", key, value)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Printf("successfully set %s=%s\n", key, value)
}

func clusterNodeInfo(w http.ResponseWriter, req *http.Request) {

	log.Println("getting cluster info....")

	slots := client.ClusterSlots(context.Background()).Val()

	err := json.NewEncoder(w).Encode(slots)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type KV struct {
	Key   string
	Value string
}
