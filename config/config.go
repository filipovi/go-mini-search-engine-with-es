package config

import (
	"encoding/json"
	"os"
)

// Config contains the information for ElasticSearch
type Config struct {
	Elastic struct {
		URL string `json:"url"`
	} `json:"elastic"`
}

// New returns a Config struct filled with the json file
func New(path string) (Config, error) {
	var cfg Config

	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		return cfg, err
	}

	if err = json.NewDecoder(file).Decode(&cfg); err != nil {
		return cfg, err
	}

	return cfg, err
}
