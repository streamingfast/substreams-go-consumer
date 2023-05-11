package main

import (
	"crypto/tls"
	"fmt"
	"regexp"

	"github.com/streamingfast/dgrpc"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
)

var portSuffixRegex = regexp.MustCompile(":[0-9]{2,5}$")

type FirehoseClientConfig struct {
	Endpoint  string
	JWT       string
	Insecure  bool
	PlainText bool
}

func NewFirehoseClient(config *FirehoseClientConfig) (cli pbfirehose.StreamClient, closeFunc func() error, err error) {
	logger := zlog.With(zap.String("endpoint", config.Endpoint))
	logger.Info("creating new client", zap.Bool("jwt_present", config.JWT != ""), zap.Bool("plaintext", config.PlainText), zap.Bool("insecure", config.Insecure))

	if !portSuffixRegex.MatchString(config.Endpoint) {
		return nil, nil, fmt.Errorf("invalid endpoint %q: endpoint's suffix must be a valid port in the form ':<port>', port 443 is usually the right one to use", config.Endpoint)
	}

	if config.PlainText && config.Insecure {
		return nil, nil, fmt.Errorf("option --insecure and --plaintext are mutually exclusive, they cannot be both specified at the same time")
	}

	var dialOptions []grpc.DialOption
	switch {
	case config.PlainText:
		logger.Debug("setting plain text option")
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	case config.Insecure:
		logger.Debug("setting insecure tls connection option")
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}))}
	}

	dialOptions = append(dialOptions,
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
	)

	if config.JWT != "" && !config.PlainText {
		logger.Debug("creating oauth access")
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(oauth.NewOauthAccess(&oauth2.Token{
			AccessToken: config.JWT,
			TokenType:   "Bearer",
		})))
	}

	logger.Debug("getting connection")
	conn, err := dgrpc.NewExternalClient(config.Endpoint, dialOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create external gRPC client: %w", err)
	}
	closeFunc = conn.Close

	logger.Debug("creating new client")
	cli = pbfirehose.NewStreamClient(conn)

	logger.Debug("client created")
	return
}
