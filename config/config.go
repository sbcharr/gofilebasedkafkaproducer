package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// AppConfig is the main config file for the app
type AppConfig struct {
	Table       string `json:"table_name"`
	FileLoc     string `json:"file_loc"`
	Topic       string `json:"kafka_topic"`
	Compression string `json:"compression"`
	Enabled     bool   `json:"enabled"`
}

//NewAppConfig returns an AppConfig object
// func NewAppConfig() *AppConfig {
// 	conf := AppConfig{}

// 	return &conf
// }

// LoadConfig loads the overall config for the app
func LoadConfig() (conf []*AppConfig, err error) {
	jsonFile, err := os.Open("config/config.json")
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(byteValue, &conf)

	return conf, nil
}
