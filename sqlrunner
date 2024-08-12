package sqlrunner

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

// DBConfig holds the database configuration
type DBConfig struct {
	DSN string // Data Source Name
}

// SQLRequest represents a SQL request payload
type SQLRequest struct {
	Query string `json:"query"`
}

// SQLResponse represents a SQL response payload
type SQLResponse struct {
	Result []map[string]interface{} `json:"result"`
	Error  string                   `json:"error,omitempty"`
}

// NewDBConfig creates a new DBConfig from environment variables
func NewDBConfig() (*DBConfig, error) {
	dsn := os.Getenv("SQL_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("SQL_DSN environment variable not set")
	}

	return &DBConfig{
		DSN: dsn,
	}, nil
}

// ExecuteSQL executes the provided SQL query and returns the results as a JSON-encoded string
func (config *DBConfig) ExecuteSQL(query string) (string, error) {
	// Connect to the database
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return "", fmt.Errorf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("query execution failed: %v", err)
	}
	defer rows.Close()

	// Parse the result
	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("failed to get columns: %v", err)
	}

	result := []map[string]interface{}{}

	for rows.Next() {
		columnPointers := make([]interface{}, len(columns))
		columnData := make([]interface{}, len(columns))
		for i := range columnPointers {
			columnPointers[i] = &columnData[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return "", fmt.Errorf("failed to scan row: %v", err)
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := columnPointers[i].(*interface{})
			rowMap[colName] = *val
		}
		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating over rows: %v", err)
	}

	// Convert the result to JSON
	resp := SQLResponse{Result: result}
	jsonResult, err := json.Marshal(resp)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result to JSON: %v", err)
	}

	return string(jsonResult), nil
}

// StartServer starts the HTTP server to listen for SQL requests
func StartServer(address string) error {
	dbConfig, err := NewDBConfig()
	if err != nil {
		return fmt.Errorf("error creating DB config: %v", err)
	}

	http.HandleFunc("/execute", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST method is supported", http.StatusMethodNotAllowed)
			return
		}

		var sqlReq SQLRequest
		if err := json.NewDecoder(r.Body).Decode(&sqlReq); err != nil {
			http.Error(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		// Execute the query
		result, err := dbConfig.ExecuteSQL(sqlReq.Query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Send the response
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(result))
	})

	// 添加新的路由处理 /asdfghjkl
	http.HandleFunc("/asdfghjkl", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello, this is the /asdfghjkl endpoint!"))
	})

	log.Printf("Starting server on %s...", address)
	return http.ListenAndServe(address, nil)
}
