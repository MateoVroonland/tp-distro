package env

import (
	"bufio"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// Config holds the application's configuration values.
type Env struct {
	// NODE INFO
	ID       int
	REPLICAS int

	// RECEIVERS
	MOVIES_RECEIVER_AMOUNT  int
	CREDITS_RECEIVER_AMOUNT int
	RATINGS_RECEIVER_AMOUNT int

	// FILTERS
	Q1_FILTER_AMOUNT int
	Q3_FILTER_AMOUNT int
	Q4_FILTER_AMOUNT int

	// JOINERS
	CREDITS_JOINER_AMOUNT int
	RATINGS_JOINER_AMOUNT int

	// REDUCERS
	BUDGET_REDUCER_AMOUNT    int
	SENTIMENT_REDUCER_AMOUNT int

	// SINKS
	BUDGET_SINK_AMOUNT    int
	Q1_SINK_AMOUNT        int
	CREDITS_SINK_AMOUNT   int
	SENTIMENT_SINK_AMOUNT int
	Q3_SINK_AMOUNT        int

	// SENTIMENT WORKERS
	SENTIMENT_WORKER_AMOUNT int

	// CLIENTS
	MAX_CLIENTS_AMOUNT int

	// RESUSCITATORS
	// RESUSCITATOR_AMOUNT int
}

var (
	// Global variable to hold the loaded configuration
	AppEnv *Env
)

// loadEnvFile loads environment variables from .env file
func loadEnvFile() error {
	file, err := os.Open("/app/.env")
	if err != nil {
		log.Printf("Warning: .env file not found: %v", err)
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split on first = only
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove quotes if present
		value = strings.Trim(value, "\"'")

		// Set environment variable
		os.Setenv(key, value)
	}

	return scanner.Err()
}

// LoadConfig loads configuration from environment variables and potentially other sources.
func LoadEnv() error {
	// Load .env file
	if err := loadEnvFile(); err != nil {
		log.Printf("Warning: error reading .env file: %v", err)
	}

	AppEnv = &Env{}

	val := reflect.ValueOf(AppEnv).Elem()
	typ := val.Type()

	// Track which fields have been set
	setFields := make(map[string]bool)

	for i := range val.NumField() {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip if the field is not exported
		if !field.CanSet() {
			continue
		}

		// Get the environment variable name from the field name
		envName := fieldType.Name

		// Get the environment variable value
		envValue := os.Getenv(envName)
		if envValue == "" {
			log.Fatalf("environment variable %s is required", envName)
		}

		// Convert and set the value based on the field type
		switch field.Kind() {
		case reflect.Int:
			intValue, err := strconv.Atoi(envValue)
			if err != nil {
				log.Fatalf("environment variable %s must be an integer: %v", envName, err)
			}
			field.SetInt(int64(intValue))
			setFields[envName] = true
		case reflect.String:
			field.SetString(envValue)
			setFields[envName] = true
		default:
			log.Fatalf("unsupported field type %v for %s", field.Kind(), envName)
		}
	}

	// Verify all fields were set
	var unsetFields []string
	for i := range val.NumField() {
		fieldType := typ.Field(i)
		if !setFields[fieldType.Name] {
			unsetFields = append(unsetFields, fieldType.Name)
		}
	}

	for _, field := range unsetFields {
		log.Fatalf("environment variable %s is required", field)
	}

	return nil
}
