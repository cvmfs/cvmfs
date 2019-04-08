package gateway

import (
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config stores all the configuration options
type Config struct {
	// Port used by the HTTP frontend
	Port int `mapstructure:"port"`
	// MaxLeaseTime is the maximum lease duration in seconds
	MaxLeaseTime int `mapstructure:"max_lease_time"`
	// UseEtcd as a consistent data store for lease information (for gateway clustering)
	UseEtcd bool `mapstructure:"use_etcd"`
	// EtcdEndpoints is a list of etcd endpoint URLs
	EtcdEndpoints []string `mapstructure:"etcd_endpoints"`
	// LogLevel sets the logging level
	LogLevel string `mapstructure:"log_level"`
	// LogTimestamps enables timestamps in the logging output
	LogTimestamps bool `mapstructure:"log_timestamps"`
	// RepoConfigFile is the file name of the repository configuration
	RepoConfigFile string `mapstructure:"repo_config_file"`
}

// ReadConfig read configuration files and populate a Config object
func ReadConfig() (*Config, error) {
	var configFile string
	pflag.StringVar(&configFile, "user_config_file", "/etc/cvmfs/gateway/user.json", "config file with user modifiable settings")
	pflag.String("repo_config_file", "/etc/cvmfs/gateway/repo.json", "repository configuration file")
	pflag.Int("port", 4929, "HTTP frontend port")
	pflag.Int("max_lease_time", 7200, "maximum lease time in seconds")
	pflag.Bool("use_etcd", false, "use etcd as a consistent data store for lease information (for gateway clustering)")
	pflag.StringSlice("etcd_endpoints", []string{}, "etcd cluster endpoints (for gateway clustering)")
	pflag.String("log_level", "info", "log level (debug|info|warn|error|fatal|panic)")
	pflag.Bool("log_timestamps", false, "enable timestamps in logging output")
	pflag.Parse()

	viper.SetConfigFile(configFile)
	viper.BindPFlags(pflag.CommandLine)
	viper.ReadInConfig()

	var conf Config
	if err := viper.Unmarshal(&conf); err != nil {
		return nil, errors.Wrap(err, "could not populate configuration object")
	}

	return &conf, nil
}
