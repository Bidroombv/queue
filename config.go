package queue

import (
	"fmt"
	"github.com/Netflix/go-env"
)

type URL struct {
	HostName string `env:"RABBITMQ_HOSTNAME,required=true"`
	Port     string `env:"RABBITMQ_PORT,required=true"`
	UserName string `env:"RABBITMQ_USERNAME,required=true"`
	Password string `env:"RABBITMQ_PASSWORD,required=true"`
	Vhost    string `env:"RABBITMQ_VHOST,required=true"`
}

func (url URL) URLString() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", url.UserName, url.Password, url.HostName, url.Port, url.Vhost)
}

func (url URL) URLStringWithoutSecrets() string {
	url.UserName = "***"
	url.Password = "***"
	return url.URLString()
}

func ReadCfgFromEnv() (*URL, error) {
	url := &URL{}
	if _, err := env.UnmarshalFromEnviron(url); err != nil {
		return nil, err
	}

	return url, nil
}
