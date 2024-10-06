package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gorilla/mux"
)

// ThongTinMonHoc represents a simple subject entity
type ThongTinMonHoc struct {
	MaMH       string    `json:"ma_mh"`
	TEnMonHoc  string    `json:"ten_mon_hoc"`
	GVCN       string    `json:"gvcn"`
	SoTinChi   int       `json:"so_tin_chi"`
	CreatedAt  time.Time `json:"created_at"`
}

var es *elasticsearch.Client

func init() {
	// Initialize Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
            "http://localhost:9200",
        },
	}
	var err error
	es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Delete the existing index if it exists
    indexName := "thongtinmonhoc"
    req := esapi.IndicesDeleteRequest{
        Index: []string{indexName},
    }
    res, err := req.Do(context.Background(), es)
    if err != nil {
        log.Fatalf("Error deleting index: %s", err)
    }
    defer res.Body.Close()

    // Create the index with the correct mapping
    createIndexReq := esapi.IndicesCreateRequest{
        Index: indexName,
        Body:  bytes.NewReader([]byte(`{
            "settings": {},
            "mappings": {
				"properties": {
				"ma_mh": {
					"type": "text"
				},
				"ten_mon_hoc": {
					"type": "text"
				},
				"gvcn": {
					"type": "text"
				},
				"so_tin_chi": {
					"type": "integer"
				},
				"created_at": {
					"type": "date"
				}
				}
			}
        }`)),
    }
    createRes, err := createIndexReq.Do(context.Background(), es)
    if err != nil {
        fmt.Printf("Error creating index: %s", err)
    }
    defer createRes.Body.Close()
    if createRes.IsError() {
        fmt.Printf("Error creating index: %s", createRes.String())
    }
}

// CreateSubject creates a new subject in Elasticsearch
func CreateSubject(w http.ResponseWriter, r *http.Request) {
	var info ThongTinMonHoc
	err := json.NewDecoder(r.Body).Decode(&info)
	if err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// Add the created_at timestamp
	info.CreatedAt = time.Now()

	// Serialize subject object to JSON
	data, err := json.Marshal(info)
	if err != nil {
		http.Error(w, "Error marshaling subject data", http.StatusInternalServerError)
		return
	}

	// Index the document in Elasticsearch
	req := esapi.IndexRequest{
		Index:      "thongtinmonhoc",
		DocumentID: info.MaMH,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil || res.IsError() {
		http.Error(w, "Error indexing document", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Subject created successfully!")
}

// GetSubject fetches a subject from Elasticsearch by ID
func GetSubject(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["ma_mh"]

	// Fetch document from Elasticsearch
	res, err := es.Get("thongtinmonhoc", id)
	if err != nil || res.IsError() {
		http.Error(w, "Subject not found", http.StatusNotFound)
		return
	}
	defer res.Body.Close()

	var subject ThongTinMonHoc
	if err := json.NewDecoder(res.Body).Decode(&subject); err != nil {
		http.Error(w, "Error decoding response", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(subject)
}

// UpdateSubject updates a subject in Elasticsearch
func UpdateSubject(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["ma_mh"]
	var updatedSubject ThongTinMonHoc

	if err := json.NewDecoder(r.Body).Decode(&updatedSubject); err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// Convert the updated subject object to JSON
	data, err := json.Marshal(updatedSubject)
	if err != nil {
		http.Error(w, "Error marshaling subject data", http.StatusInternalServerError)
		return
	}

	// Update the document in Elasticsearch
	req := esapi.UpdateRequest{
		Index:      "thongtinmonhoc",
		DocumentID: id,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil || res.IsError() {
		http.Error(w, "Error updating document", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Subject updated successfully!")
}

// DeleteSubject deletes a subject from Elasticsearch
func DeleteSubject(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["ma_mh"]

	// Delete document from Elasticsearch
	req := esapi.DeleteRequest{
		Index:      "thongtinmonhoc",
		DocumentID: id,
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil || res.IsError() {
		http.Error(w, "Error deleting subject", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Subject deleted successfully!")
}

func main() {
	// Initialize Router
	r := mux.NewRouter()

	// Define routes
	r.HandleFunc("/thongtinmonhoc", CreateSubject).Methods("POST")
	r.HandleFunc("/thongtinmonhoc/{id}", GetSubject).Methods("GET")
	r.HandleFunc("/thongtinmonhoc/{id}", UpdateSubject).Methods("PUT")
	r.HandleFunc("/thongtinmonhoc/{id}", DeleteSubject).Methods("DELETE")

	// Start server
	http.Handle("/", r)
	fmt.Println("Server running on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
