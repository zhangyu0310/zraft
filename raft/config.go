package raft

import (
	"sync/atomic"

	"zraft/log"
)

// Config Configurations of server.
type Config struct {
	ServerList         map[ZID]string
	LocalIP            string
	ServerPort         int
	HeartbeatGap       int // Millisecond
	Log                log.Log
	StateMachineEngine string
}

var (
	globalConf atomic.Value
)

// InitializeConfig initialize the global config handler.
func InitializeConfig(enforceCmdArgs func(*Config)) {
	cfg := Config{
		ServerList:         make(map[ZID]string),
		LocalIP:            "127.0.0.1",
		ServerPort:         1219,
		HeartbeatGap:       200,
		StateMachineEngine: "LevelDB",
	}
	// Use command config cover config file.
	enforceCmdArgs(&cfg)
	StoreGlobalConfig(&cfg)
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
}
