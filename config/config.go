package config

import (
	"github.com/spf13/viper"
	"log"
	"os"
)

type Config struct {
	ProducerFailNumber   int   `mapstructure:"PRODUCER_FAIL_NUMBER"`
	ProducerPanicNumber  int   `mapstructure:"PRODUCER_PANIC_NUMBER"`
	ProducerSendInternal int64 `mapstructure:"PRODUCER_SEND_INTERVAL"`
	ConsumerCount        int   `mapstructure:"CONSUMER_COUNT"`
	ConsumerFailNumber   int   `mapstructure:"CONSUMER_FAIL_NUMBER"`
	ConsumerPanicNumber  int   `mapstructure:"CONSUMER_PANIC_NUMBER"`
	ConsumerReadInternal int64 `mapstructure:"CONSUMER_READ_INTERVAL"`
}

var (
	cfg Config
)

func initDefaultValue() {
	viper.SetDefault("PRODUCER_FAIL_NUMBER", -1)
	viper.SetDefault("PRODUCER_PANIC_NUMBER", -1)
	viper.SetDefault("PRODUCER_SEND_INTERVAL", 10) // in milliseconds
	viper.SetDefault("CONSUMER_COUNT", 4)
	viper.SetDefault("CONSUMER_FAIL_NUMBER", -1)
	viper.SetDefault("CONSUMER_PANIC_NUMBER", -1)
	viper.SetDefault("CONSUMER_READ_INTERVAL", 0)
}

func init() {
	initDefaultValue()
}

func InitConfig() *Config {
	configFile := os.Getenv("CONFIG_FILE")
	if len(configFile) == 0 {
		configFile = "config.json"
	}
	log.Printf("config file: %v", configFile)

	viper.AddConfigPath("./")
	viper.SetConfigFile(configFile)

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("fail to read config file %v, error: %v", configFile, err)
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		log.Panicf("fail to unmarshal config, error: %v", err)
	}

	log.Printf("config file loaded, config: %#v", cfg)

	return &cfg
}

func GetConfig() *Config {
	return &cfg
}
