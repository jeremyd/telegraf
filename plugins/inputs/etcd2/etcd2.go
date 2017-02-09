package etcd

import (
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Etcd2 struct {
	Urls        []string
	SSLCAFile   string `toml:"ssl_ca_file"`
	SSLCertFile string `toml:"ssl_cert_file"`
	SSLKeyFile  string `toml:"ssl_key_file"`
	Cluster     string `toml:"cluster"`
}

var sampleConfig = `
    ## Etcd Urls
    # Urls = ["https://localhost:2379"]
    ## path to cert file
    # ssl_cert_file = "/etc/ssl/etcd.pem"
    ## path to ca file
    # ssl_ca_file = "/etc/ssl/ca.pem"
    ## path to key file
    # ssl_key_file = "/etc/ssl/etcd-key.pem"
    ## cluster name (tag)
    # cluster = "development"
`

func (e *Etcd2) Description() string {
	return "gather etcd cluster health"
}

func (e *Etcd2) SampleConfig() string {
	return sampleConfig
}

// Connect to an Etcd Endpoint and return the client.
func (e *Etcd2) connectTo(endpoints []string) (client.Client, error) {
	certFile := e.SSLCertFile
	caFile := e.SSLCAFile
	keyFile := e.SSLKeyFile
	tls := transport.TLSInfo{
		CAFile:   caFile,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
	tlsTimeout := 10 * time.Second
	tr, trErr := transport.NewTransport(tls, tlsTimeout)
	if trErr != nil {
		return nil, trErr
	}
	cfg := client.Config{
		Transport:               tr,
		Endpoints:               endpoints,
		HeaderTimeoutPerRequest: tlsTimeout,
	}
	cln, clientError := client.New(cfg)
	if clientError != nil {
		return cln, clientError
	}
	return cln, nil
}

func (e *Etcd2) Gather(acc telegraf.Accumulator) error {
	record := make(map[string]interface{})
	tags := make(map[string]string)
	cln, clientErr := e.connectTo(e.Urls)

	if clientErr != nil {
		return clientErr
	}

	mi := client.NewMembersAPI(cln)
	memberContext, memberCancel := context.WithTimeout(context.TODO(), 20*time.Second)
	defer memberCancel()
	ms, err := mi.List(memberContext)
	if err != nil {
		return err
	}
	// Perform health check on each member
	for _, ep := range ms {
		// 0 is healthy and 1 is unhealthy (so that we can be compatible with DataDog output)
		var isHealthy int
		thisEp := "https://" + ep.Name + ":2379"
		start := time.Now()
		cli, err := e.connectTo([]string{thisEp})
		if err != nil {
			isHealthy = 0
		} else {
			keysAPI := client.NewKeysAPI(cli)
			ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Second)
			defer cancel()
			_, err := keysAPI.Set(ctx, "/telegrafetcd", "telegrafetcd", nil)
			if err != nil {
				isHealthy = 0
			} else {
				isHealthy = 1
			}
		}
		record["response_time"] = time.Since(start).Seconds() * 1000
		record["is_healthy"] = isHealthy
		tags["name"] = ep.Name
		tags["hostname"] = ep.Name
		tags["id"] = ep.ID
		tags["cluster"] = e.Cluster
		acc.AddFields("etcd_health_checks", record, tags)
	}
	return nil
}

func init() {
	inputs.Add("etcd2", func() telegraf.Input {
		return &Etcd2{}
	})
}
