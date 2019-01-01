package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type TopicMissingError struct {
	msg string
}

func (e TopicMissingError) Error() string { return e.msg }

type Client struct {
	client      sarama.Client
	kafkaConfig *sarama.Config
	config      *Config
}

type Config struct {
	BootstrapServers *[]string
	Timeout          int
	CACert           *x509.Certificate
	CACertFile       string
	ClientCert       *tls.Certificate
	ClientCertFile   string
	ClientCertKey    string
	TLSEnabled       bool
	SkipTLSVerify    bool
	SASLUsername     string
	SASLPassword     string
}

func (c *Config) String() string {
	return fmt.Sprintf("BootstrapServers: %s\nTimeout: %d,\nTLS: %v,SkipVerify: %v", *c.BootstrapServers, c.Timeout, c.TLSEnabled, c.SkipTLSVerify)
}

func (c *Config) SASLEnabled() bool {
	return c.SASLUsername != "" || c.SASLPassword != ""
}

func NewClient(config *Config) (*Client, error) {
	bootstrapServers := *(config.BootstrapServers)

	if bootstrapServers == nil {
		return nil, fmt.Errorf("No bootstrap_servers provided")
	}

	kc, err := config.newKafkaConfig()
	if err != nil {
		log.Println("[ERROR] Error creating kafka client")
		return nil, err
	}

	c, err := sarama.NewClient(bootstrapServers, kc)
	if err != nil {
		log.Println("[ERROR] Error connecting to kafka")
		return nil, err
	}

	sarama.Logger = log.New(os.Stdout, "[TRACE] [Sarama]", log.LstdFlags)
	return &Client{
		client:      c,
		config:      config,
		kafkaConfig: kc,
	}, kc.Validate()
}

func (c *Client) DeleteTopic(t string) error {
	broker, err := c.client.Controller()

	if err != nil {
		return err
	}

	timeout := time.Duration(c.config.Timeout) * time.Second
	req := &sarama.DeleteTopicsRequest{
		Topics:  []string{t},
		Timeout: timeout,
	}
	res, err := broker.DeleteTopics(req)

	if err == nil {
		for k, e := range res.TopicErrorCodes {
			if e != sarama.ErrNoError {
				return fmt.Errorf("%s : %s", k, e)
			}
		}
	} else {
		log.Printf("[ERROR] Error deleting topic %s from Kafka: %s", t, err)
		return err
	}

	log.Printf("[INFO] Deleted topic %s from Kafka", t)

	return nil
}

func (c *Client) UpdateTopic(topic Topic) error {
	broker, err := c.client.Controller()

	if err != nil {
		return err
	}

	r := &sarama.AlterConfigsRequest{
		Resources:    configToResources(topic),
		ValidateOnly: false,
	}

	res, err := broker.AlterConfigs(r)

	if err != nil {
		return err
	}

	if err == nil {
		for _, e := range res.Resources {
			if e.ErrorCode != int16(sarama.ErrNoError) {
				return errors.New(e.ErrorMsg)
			}
		}
	}

	return nil
}

func (c *Client) CreateTopic(t Topic) error {
	broker, err := c.client.Controller()

	if err != nil {
		log.Printf("[WARN] Could get an available broker %s", err)
		return err
	}

	timeout := time.Duration(c.config.Timeout) * time.Second
	log.Printf("[DEBUG] Timeout is %v ", timeout)
	req := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			t.Name: {
				NumPartitions:     t.Partitions,
				ReplicationFactor: t.ReplicationFactor,
				ConfigEntries:     t.Config,
			},
		},
		Timeout: timeout,
	}
	res, err := broker.CreateTopics(req)

	if err == nil {
		for _, e := range res.TopicErrors {
			if e.Err != sarama.ErrNoError {
				return fmt.Errorf("%s", e.Err)
			}
		}
		log.Printf("[INFO] Created topic %s in Kafka", t.Name)
	}

	return err
}

func (c *Client) AddPartitions(t Topic) error {
	broker, err := c.client.Controller()
	if err != nil {
		log.Printf("[ERROR] Unable to fetch controller: %s", err)
		return err
	}

	timeout := time.Duration(c.config.Timeout) * time.Second

	tp := map[string]*sarama.TopicPartition{
		t.Name: &sarama.TopicPartition{
			Count: t.Partitions,
		},
	}

	req := &sarama.CreatePartitionsRequest{
		TopicPartitions: tp,
		Timeout:         timeout,
		ValidateOnly:    false,
	}
	log.Printf("[INFO] Adding partitions to %s in Kafka", t.Name)
	res, err := broker.CreatePartitions(req)
	if err == nil {
		for _, e := range res.TopicPartitionErrors {
			if e.Err != sarama.ErrNoError {
				return fmt.Errorf("%s", e.Err)
			}
		}
		log.Printf("[INFO] Added partitions to %s in Kafka", t.Name)
	}

	return err
}

func (client *Client) ReadTopic(name string) (Topic, error) {
	c := client.client

	topic := Topic{
		Name: name,
	}

	err := c.RefreshMetadata()
	topics, err := c.Topics()

	if err != nil {
		log.Printf("[ERROR] Error getting topics %s from Kafka", err)
		return topic, err
	}

	for _, t := range topics {
		log.Printf("[DEBUG] Reading Topic %s from Kafka", t)
		if name == t {
			log.Printf("[DEBUG] FOUND %s from Kafka", t)
			p, err := c.Partitions(t)
			if err == nil {
				partitionCount := int32(len(p))
				log.Printf("[DEBUG] %d Partitions Found: %v from Kafka", partitionCount, p)
				topic.Partitions = partitionCount

				r, err := ReplicaCount(c, name, p)
				if err == nil {
					log.Printf("[DEBUG] ReplicationFactor %d from Kafka", r)
					topic.ReplicationFactor = int16(r)
				}

				configToSave, err := client.topicConfig(t)
				if err != nil {
					log.Printf("[ERROR] Could not get config for topic %s: %s", t, err)
					return topic, err
				}

				log.Printf("[DEBUG] Config %v from Kafka", strPtrMapToStrMap(configToSave))
				topic.Config = configToSave
				return topic, nil
			}
		}
	}
	err = TopicMissingError{msg: fmt.Sprintf("%s could not be found", name)}
	return topic, err
}

func (c *Client) CreateACL(s stringlyTypedACL) error {
	broker, err := c.availableBroker()
	if err != nil {
		return err
	}

	ac, err := s.AclCreation()
	if err != nil {
		return err
	}
	req := &sarama.CreateAclsRequest{
		Version:      1,
		AclCreations: []*sarama.AclCreation{ac},
	}

	res, err := broker.CreateAcls(req)
	if err != nil {
		return err
	}

	for _, r := range res.AclCreationResponses {
		if r.Err != sarama.ErrNoError {
			return r.Err
		}
	}

	return nil
}

func (c *Client) ListACLs() ([]*sarama.ResourceAcls, error) {
	broker, err := c.availableBroker()
	if err != nil {
		return nil, err
	}
	err = c.client.RefreshMetadata()
	if err != nil {
		return nil, err
	}
	allResources := []*sarama.DescribeAclsRequest{
		&sarama.DescribeAclsRequest{
			Version: 1,
			AclFilter: sarama.AclFilter{
				ResourceType:              sarama.AclResourceTopic,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				PermissionType:            sarama.AclPermissionAny,
				Operation:                 sarama.AclOperationAny,
			},
		},
		&sarama.DescribeAclsRequest{
			Version: 1,
			AclFilter: sarama.AclFilter{
				ResourceType:              sarama.AclResourceGroup,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				PermissionType:            sarama.AclPermissionAny,
				Operation:                 sarama.AclOperationAny,
			},
		},
		&sarama.DescribeAclsRequest{
			Version: 1,
			AclFilter: sarama.AclFilter{
				ResourceType:              sarama.AclResourceCluster,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				PermissionType:            sarama.AclPermissionAny,
				Operation:                 sarama.AclOperationAny,
			},
		},
		&sarama.DescribeAclsRequest{
			Version: 1,
			AclFilter: sarama.AclFilter{
				ResourceType:              sarama.AclResourceTransactionalID,
				ResourcePatternTypeFilter: sarama.AclPatternAny,
				PermissionType:            sarama.AclPermissionAny,
				Operation:                 sarama.AclOperationAny,
			},
		},
	}
	res := []*sarama.ResourceAcls{}

	for _, r := range allResources {
		aclsR, err := broker.DescribeAcls(r)
		if err != nil {
			return nil, err
		}

		if err == nil {
			if aclsR.Err != sarama.ErrNoError {
				return nil, fmt.Errorf("%s", aclsR.Err)
			}
		}

		for _, a := range aclsR.ResourceAcls {
			res = append(res, a)
		}
	}
	return res, err
}

func (c *Client) topicConfig(topic string) (map[string]*string, error) {
	conf := map[string]*string{}
	request := &sarama.DescribeConfigsRequest{
		Version: 1,
		Resources: []*sarama.ConfigResource{
			{
				Type: sarama.TopicResource,
				Name: topic,
			},
		},
	}

	broker, err := c.client.Controller()
	if err != nil {
		return conf, err
	}

	cr, err := broker.DescribeConfigs(request)
	if err != nil {
		return conf, err
	}

	if len(cr.Resources) > 0 && len(cr.Resources[0].Configs) > 0 {
		for _, tConf := range cr.Resources[0].Configs {
			v := tConf.Value
			log.Printf("[INFO] Topic: %s. %s: %v. Default %v, Source %v, Version %d", topic, tConf.Name, v, tConf.Default, tConf.Source, cr.Version)
			for _, s := range tConf.Synonyms {
				log.Printf("[INFO] Syonyms: %v", s)
			}

			if isDefault(tConf, int(cr.Version)) {
				continue
			}
			conf[tConf.Name] = &v
		}
	}
	return conf, nil
}

func isDefault(tc *sarama.ConfigEntry, version int) bool {
	if version == 0 {
		return tc.Default
	}
	return tc.Source == sarama.SourceDefault || tc.Source == sarama.SourceStaticBroker
}

func (c *Client) availableBroker() (*sarama.Broker, error) {
	var err error
	brokers := *c.config.BootstrapServers
	kc := c.kafkaConfig

	log.Printf("[DEBUG] Looking for Brokers @ %v", brokers)
	for _, b := range brokers {
		broker := sarama.NewBroker(b)
		err = broker.Open(kc)
		if err == nil {
			return broker, nil
		}
		log.Printf("[WARN] Broker @ %s cannot be reached\n", b)
	}

	return nil, fmt.Errorf("No Available Brokers @ %v", brokers)
}

func (c *Config) newKafkaConfig() (*sarama.Config, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_0_0_0
	kafkaConfig.ClientID = "terraform-provider-kafka"

	if c.SASLEnabled() {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Password = c.SASLPassword
		kafkaConfig.Net.SASL.User = c.SASLUsername
	}

	if c.TLSEnabled {
		tlsConfig, err := c.newTLSConfig()
		if err != nil {
			return kafkaConfig, err
		}

		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = tlsConfig
		kafkaConfig.Net.TLS.Config.InsecureSkipVerify = c.SkipTLSVerify
	}

	return kafkaConfig, nil
}

func (c *Config) newTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	cert, err := c.clientCert()
	if err != nil {
		return tlsConfig, err
	}
	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	pool, err := c.caCertPool()
	if err != nil {
		return tlsConfig, err
	}
	if pool != nil {
		tlsConfig.RootCAs = pool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (c *Config) clientCert() (*tls.Certificate, error) {
	if c.ClientCert != nil {
		return c.ClientCert, nil
	}
	if c.ClientCertFile != "" && c.ClientCertKey != "" {
		cert, err := tls.LoadX509KeyPair(c.ClientCertFile, c.ClientCertKey)
		if err != nil {
			return nil, err
		}
		return &cert, nil
	}

	return nil, nil
}

func (c *Config) caCertPool() (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if c.CACert != nil {
		pool.AddCert(c.CACert)
	} else if c.CACertFile == "" {
		caCert, err := ioutil.ReadFile(c.CACertFile)
		if err != nil {
			return nil, err
		}
		pool.AppendCertsFromPEM(caCert)
	}
	return pool, nil
}
