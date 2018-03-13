package crud_tests

import (
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"testing"
)

type testConfig struct {
	TimeouScale             float64  `json:"timeout_scale"`
	CassPwd                 string   `json:"cassandra_password"`
	NativePort              string   `json:"cassandra_native_transport_port"`
	Servers                 []string `json:"cassandra_servers"`
	Seeds                   []string `json:"cassandra_seeds"`
	ClusterName             string   `json:"cluster_name"`
	ClientEncryptionEnabled bool     `json:"client_encryption.enabled"`
	Validate_SSL_TF         bool     `json:"validate_ssl_TF"`
	RfFactor                int      `json:"keyspace_replication_factor"`
	ReplStrat               string   `json:"replication_strategy"`
	DurableW                bool     `json:"durable_write"`
}

func loadConfig(path string) (cfg testConfig) {
	configFile, err := os.Open(path)
	if err != nil {
		fatal(err)
	}

	decoder := json.NewDecoder(configFile)
	if err = decoder.Decode(&cfg); err != nil {
		fatal(err)
	}
	return
}

var (
	config = loadConfig(os.Getenv("CONFIG_PATH"))
)

func fatal(err error) {
	fmt.Printf("ERROR: %s\n", err.Error())
	os.Exit(1)
}

func TestReadwrite(t *testing.T) {

	RegisterFailHandler(Fail)

	RunSpecs(t, "Cassandra Acceptance Tests")
}
