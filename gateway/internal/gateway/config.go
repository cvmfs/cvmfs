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
	// Port used by pprof
	PProfPort int `mapstructure:"pprof_port"`
	// If PProfPort is already in use, allow to use ports up to this value
	PProfPortRangeMax int `mapstructure:"pprof_port_range_max"`
	// MaxLeaseTime is the initial lease duration in seconds. It also caps the
	// extension that a refresh (PATCH /leases/:token) can apply.
	MaxLeaseTime time.Duration `mapstructure:"max_lease_time"`
	// LeaseRefreshTime is the default duration (in seconds) by which an active
	// lease's expiration is extended when it is refreshed via
	// PATCH /leases/:token without an explicit expires_in_sec. It is capped by
	// MaxLeaseTime. If unset (0), it defaults to MaxLeaseTime.
	LeaseRefreshTime time.Duration `mapstructure:"lease_refresh_time"`
	// CommitLeaseExpiryMargin is a safety margin (in seconds) subtracted from the
	// lease expiration before a commit is published: if the lease would expire
	// within this margin, the commit is refused. This guards against an
	// overlapping lease being granted to another publisher as the lease expires
	// during a slow commit.
	CommitLeaseExpiryMargin time.Duration `mapstructure:"commit_lease_expiry_margin"`
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
	// EnableKeyEndpoint enables the /repos/:name/keys endpoint that allows
	// publishers to fetch the repository public key and certificate
	EnableKeyEndpoint bool `mapstructure:"enable_key_endpoint"`
}

// ReadConfig reads configuration files and commandline flags, and populates a Config object
func ReadConfig() (*Config, error) {
	var configFile string
	pflag.StringVar(&configFile, "user_config_file", "/etc/cvmfs/gateway/user.json", "config file with user modifiable settings")
	pflag.String("access_config_file", "/etc/cvmfs/gateway/repo.json", "repository access configuration file")
	pflag.Int("port", 4929, "HTTP frontend port")
	pflag.Int("pprof_port", 6060, "pprof port on localhost")
	pflag.Int("pprof_port_range_max", 6260, "pprof port on localhost")
	pflag.Int("lease_refresh_time", 0, "lease extension in seconds applied on refresh (0: use max_lease_time)")
	pflag.Int("max_lease_time", 7200, "maximum lease time in seconds")
	pflag.Int("commit_lease_expiry_margin", 1, "safety margin in seconds before lease expiry within which a commit is refused")
	pflag.String("log_level", "info", "log level (debug|info|warn|error|fatal|panic)")
	pflag.Bool("log_timestamps", false, "enable timestamps in logging output")
	pflag.Int("num_receivers", 1, "number of parallel cvmfs_receiver processes to run")
	pflag.String("receiver_path", "/usr/bin/cvmfs_receiver", "the path of the cvmfs_receiver executable")
	pflag.String("work_dir", "/var/lib/cvmfs-gateway", "the working directory for database files")
	pflag.Bool("mock_receiver", false, "enable the mocked implementation of the receiver process (for testing)")
	pflag.Bool("enable_key_endpoint", false, "enable the /repos/:name/keys endpoint for publisher key retrieval")
	pflag.Parse()

	viper.SetConfigFile(configFile)
	viper.BindPFlags(pflag.CommandLine)
	viper.ReadInConfig()

	var conf Config
	if err := viper.Unmarshal(&conf); err != nil {
		return nil, fmt.Errorf("could not populate configuration object: %w", err)
	}

	// max_lease_time is given in seconds in the config file or at the command line
	conf.MaxLeaseTime = conf.MaxLeaseTime * time.Second
	// commit_lease_expiry_margin is likewise given in seconds
	conf.CommitLeaseExpiryMargin = conf.CommitLeaseExpiryMargin * time.Second

	// lease_refresh_time is given in seconds; if unset it falls back to max_lease_time
	conf.LeaseRefreshTime = conf.LeaseRefreshTime * time.Second
	if conf.LeaseRefreshTime == 0 {
		conf.LeaseRefreshTime = conf.MaxLeaseTime
	}

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
			return nil, fmt.Errorf("could not load receiver config: %w", err)
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
			return nil, fmt.Errorf("could not load receiver config: %w", err)
		}
		if !pflag.CommandLine.Changed("receiver_path") {
			conf.ReceiverPath = sc2.Executable
		}
	}

	return &conf, nil
}
