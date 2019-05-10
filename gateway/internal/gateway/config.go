package gateway

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config stores all the configuration options
type Config struct {
	// Port used by the HTTP frontend
	Port int `mapstructure:"port"`
	// MaxLeaseTime is the maximum lease duration in seconds
	MaxLeaseTime time.Duration `mapstructure:"max_lease_time"`
	// LeaseDB backend boltdb, sqlite, or etcd
	LeaseDB string `mapstructure:"lease_db"`
	// EtcdEndpoints is a list of etcd endpoint URLs
	EtcdEndpoints []string `mapstructure:"etcd_endpoints"`
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

// ReadConfig reads configuration files and commandline flags, and populates a Config object
func ReadConfig() (*Config, error) {
	var configFile string
	pflag.StringVar(&configFile, "user_config_file", "/etc/cvmfs/gateway/user.json", "config file with user modifiable settings")
	pflag.String("access_config_file", "/etc/cvmfs/gateway/repo.json", "repository access configuration file")
	pflag.Int("port", 4929, "HTTP frontend port")
	pflag.Int("max_lease_time", 7200, "maximum lease time in seconds")
	pflag.String("lease_db", "boltdb", "lease DB backend to use: boltdb, sqlite, or etcd (default boltdb)")
	pflag.Bool("use_etcd", false, "use etcd as a consistent data store for lease information (for gateway clustering)")
	pflag.StringSlice("etcd_endpoints", []string{}, "etcd cluster endpoints (for gateway clustering)")
	pflag.String("log_level", "info", "log level (debug|info|warn|error|fatal|panic)")
	pflag.Bool("log_timestamps", false, "enable timestamps in logging output")
	pflag.Int("num_receivers", 1, "number of parallel cvmfs_receiver processes to run")
	pflag.String("receiver_path", "/usr/bin/cvmfs_receiver", "the path of the cvmfs_receiver executable")
	pflag.String("work_dir", "/var/lib/cvmfs-gateway", "the working directory for database files")
	pflag.Bool("mock_receiver", false, "enable the mocked implementation of the receiver process (for testing)")
	pflag.Parse()

	viper.SetConfigFile(configFile)
	viper.BindPFlags(pflag.CommandLine)
	viper.ReadInConfig()

	var conf Config
	if err := viper.Unmarshal(&conf); err != nil {
		return nil, errors.Wrap(err, "could not populate configuration object")
	}

	// max_lease_time is given in seconds in the config file or at the command line
	conf.MaxLeaseTime = conf.MaxLeaseTime * time.Second

	// Manually handler legacy parameter names

	if viper.InConfig("fe_tcp_port") {
		conf.Port = viper.GetInt("fe_tcp_port")
	}

	var sc1 struct {
		Size int `mapstructure:"size"`
	}
	v1 := viper.Sub("receiver_config")
	if v1 != nil {
		if err := v1.Unmarshal(&sc1); err != nil {
			return nil, errors.Wrap(err, "could not load receiver config")
		}
		if !pflag.CommandLine.Changed("num_receivers") {
			conf.NumReceivers = sc1.Size
		}
	}

	var sc2 struct {
		Executable string `mapstructure:"executable_path"`
	}
	v2 := viper.Sub("receiver_worker_config")
	if v2 != nil {
		if err := v2.Unmarshal(&sc2); err != nil {
			return nil, errors.Wrap(err, "could not load receiver config")
		}
		if !pflag.CommandLine.Changed("receiver_path") {
			conf.ReceiverPath = sc2.Executable
		}
	}

	return &conf, nil
}
