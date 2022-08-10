package gateway

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config stores all the configuration options
type Config struct {
	// Port used by the HTTP frontend
	Port int `mapstructure:"port"`
	// MaxLeaseTime is the maximum lease duration in seconds
	MaxLeaseTime time.Duration `mapstructure:"max_lease_time"`
	// LogLevel sets the logging level
	LogLevel string `mapstructure:"log_level"`
	// LogTimestamps enables timestamps in the logging output
	LogTimestamps bool `mapstructure:"log_timestamps"`
	// AccessConfigFile is the file name of the repository access configuration
	AccessConfigFile string `mapstructure:"access_config_file"`
	// NumReceivers is the number of parallel (receiver) workers to run
	NumReceivers int `mapstructure:"num_receivers"`
	// ReceiverPath is the path of the cvmfs_receiver executable
	ReceiverPath string `mapstructure:"receiver_path"`
	// WorkDir is where the lease BD stores its data
	WorkDir string `mapstructure:"work_dir"`
	// MockReceiver enables a mocked implementation of the receiver worker
	MockReceiver bool `mapstructure:"mock_receiver"`
}

var ConfigFile string

// ReadConfig reads configuration files and commandline flags, and populates a Config object
func ReadConfig() (*Config, error) {
	viper.SetConfigFile(ConfigFile)
	viper.ReadInConfig()

	var conf Config
	if err := viper.Unmarshal(&conf); err != nil {
		return nil, fmt.Errorf("could not populate configuration object: %w", err)
	}

	// max_lease_time is given in seconds in the config file or at the command line
	conf.MaxLeaseTime = conf.MaxLeaseTime * time.Second

	// Manually handle legacy parameter names

	if viper.InConfig("fe_tcp_port") {
		conf.Port = viper.GetInt("fe_tcp_port")
	}

	var sc1 struct {
		Size int `mapstructure:"size"`
	}
	v1 := viper.Sub("receiver_config")
	if v1 != nil {
		if err := v1.Unmarshal(&sc1); err != nil {
			return nil, fmt.Errorf("could not load receiver config: %w", err)
		}
		if !viper.IsSet("num_receivers") {
			conf.NumReceivers = sc1.Size
		}
	}

	var sc2 struct {
		Executable string `mapstructure:"executable_path"`
	}
	v2 := viper.Sub("receiver_worker_config")
	if v2 != nil {
		if err := v2.Unmarshal(&sc2); err != nil {
			return nil, fmt.Errorf("could not load receiver config: %w", err)
		}
		if !pflag.CommandLine.Changed("receiver_path") {
			conf.ReceiverPath = sc2.Executable
		}
	}

	return &conf, nil
}
