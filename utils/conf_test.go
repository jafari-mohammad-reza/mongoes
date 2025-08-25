package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfWithPartialConfig(t *testing.T) {
	// Backup original config if it exists
	var originalExists bool
	_, err := os.ReadFile("config.yaml")
	if err == nil {
		originalExists = true
		err = os.Rename("config.yaml", "config.yaml.bak")
		assert.NoError(t, err)
	}

	// Create a temporary config file with only one field
	configContent := `mongo:
  url: mongodb://localhost:27018
`

	// Write temporary config file
	err = os.WriteFile("config.yaml", []byte(configContent), 0644)
	assert.NoError(t, err)

	defer func() {
		os.Remove("config.yaml")
		if originalExists {
			os.Rename("config.yaml.bak", "config.yaml")
		}
	}()

	// Test with partial config using real NewConf function
	cfg, err := NewConf()
	assert.NoError(t, err)

	// Verify that URL is overridden
	assert.Equal(t, "mongodb://localhost:27018", cfg.Mongo.URL)

	// Verify that defaults are preserved for missing fields
	assert.Equal(t, 10, cfg.Mongo.BatchTimeoutSec)
	assert.Equal(t, "test", cfg.Mongo.DB)
	assert.Equal(t, []string{}, cfg.Mongo.WhiteList)

	// Verify elastic defaults
	assert.Equal(t, []string{"http://localhost:9200"}, cfg.Elastic.Addresses)
	assert.Equal(t, "", cfg.Elastic.User)
	assert.Equal(t, "", cfg.Elastic.Password)
	assert.NotNil(t, cfg.Elastic.UniqueFields)
	assert.NotNil(t, cfg.Elastic.IndicPeriod)
	assert.NotNil(t, cfg.Elastic.CollPrefix)
}

func TestNewConfWithEmptyConfig(t *testing.T) {
	// Backup original config if it exists
	var originalExists bool
	_, err := os.ReadFile("config.yaml")
	if err == nil {
		originalExists = true
		err = os.Rename("config.yaml", "config.yaml.bak")
		assert.NoError(t, err)
	}

	// Create empty config file
	configContent := ``

	err = os.WriteFile("config.yaml", []byte(configContent), 0644)
	assert.NoError(t, err)

	defer func() {
		os.Remove("config.yaml")
		if originalExists {
			os.Rename("config.yaml.bak", "config.yaml")
		}
	}()

	cfg, err := NewConf()
	assert.NoError(t, err)

	// All values should be defaults
	assert.Equal(t, "mongodb://localhost:27017", cfg.Mongo.URL)
	assert.Equal(t, 10, cfg.Mongo.BatchTimeoutSec)
	assert.Equal(t, "test", cfg.Mongo.DB)
	assert.Equal(t, []string{}, cfg.Mongo.WhiteList)
	assert.NotNil(t, cfg.Mongo.CollBatch)

	assert.Equal(t, []string{"http://localhost:9200"}, cfg.Elastic.Addresses)
	assert.Equal(t, "", cfg.Elastic.User)
	assert.Equal(t, "", cfg.Elastic.Password)
	assert.NotNil(t, cfg.Elastic.UniqueFields)
	assert.NotNil(t, cfg.Elastic.IndicPeriod)
	assert.NotNil(t, cfg.Elastic.CollPrefix)
}

func TestNewConfNoConfigFile(t *testing.T) {
	// Test fallback when config file doesn't exist
	cfg, err := NewConf()
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Should have default values
	assert.Equal(t, "mongodb://localhost:27017", cfg.Mongo.URL)
	assert.Equal(t, 10, cfg.Mongo.BatchTimeoutSec)
	assert.Equal(t, "test", cfg.Mongo.DB)
	assert.Equal(t, []string{}, cfg.Mongo.WhiteList)
	assert.NotNil(t, cfg.Mongo.CollBatch)

	assert.Equal(t, []string{"http://localhost:9200"}, cfg.Elastic.Addresses)
	assert.Equal(t, "", cfg.Elastic.User)
	assert.Equal(t, "", cfg.Elastic.Password)
	assert.NotNil(t, cfg.Elastic.UniqueFields)
	assert.NotNil(t, cfg.Elastic.IndicPeriod)
	assert.NotNil(t, cfg.Elastic.CollPrefix)
}
