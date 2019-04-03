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
	// EtcdEndpoints is a list of etcd endpoint URLs
	EtcdEndpoints []string `mapstructure:"etcd_endpoints"`
	// S3Endpoint is the URL of the S3 storage backend
	S3Endpoint string `mapstructure:"s3_endpoint"`
	// S3ForcePathStyle bucket specification for Minio
	S3ForcePathStyle bool `mapstructure:"s3_force_path_style"`
	// S3DisableSSL to connect using HTTP
	S3DisableSSL bool `mapstructure:"s3_disable_ssl"`
	// LogLevel sets the logging level
	LogLevel string `mapstructure:"log_level"`
	// LogTimestamps enables timestamps in the logging output
	LogTimestamps bool `mapstructure:"log_timestamps"`
}

// ReadConfig read configuration files and populate a Config object
func ReadConfig() (*Config, error) {
	viper.SetConfigName("user")
	viper.AddConfigPath("/etc/cvmfs/gateway")
	viper.AddConfigPath("./config")

	pflag.Int("port", 4929, "HTTP frontend port")
	pflag.Int("max_lease_time", 7200, "maximum lease time in seconds")
	pflag.StringSlice("etcd_endpoints", []string{"localhost:2379"}, "etcd cluster endpoints")
	pflag.String("s3_endpoint", "localhost:9000", "S3 storage backend endpoint")
	pflag.Bool("s3_force_path_style", true, "force path-style S3 URL")
	pflag.Bool("s3_disable_ssl", true, "disable SSL for accessing S3")
	pflag.String("log_level", "info", "log level (debug|info|warn|error|fatal|panic)")
	pflag.Bool("log_timestamps", false, "enable timestamps in logging output")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	viper.ReadInConfig()

	var conf Config
	if err := viper.Unmarshal(&conf); err != nil {
		return nil, errors.Wrap(err, "could not populate configuration object")
	}

	return &conf, nil
}
