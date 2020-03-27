package lib

import (
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/sirupsen/logrus"
)

var (
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc = athena.New(sess)
)

func init() {
	lvl, ok := os.LookupEnv("LOG_LEVEL")
	// LOG_LEVEL not set, let's default to info
	if !ok {
		lvl = "info"
	}
	// parse string, this is built-in feature of logrus
	ll, err := logrus.ParseLevel(lvl)
	if err != nil {
		ll = logrus.DebugLevel
	}
	// set global log level
	logrus.SetLevel(ll)
	logrus.SetOutput(os.Stderr)
}

// AccountID returns the current AWS Account ID
func AccountID() string {
	svc := sts.New(sess)

	result, err := svc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		logrus.Debugf("unable to get AWS Account ID: %v\n", err)
	}

	if result.Account != nil {
		return *result.Account
	}

	return ""
}

// Region returns the current region
func Region() string {
	if sess.Config.Region != nil {
		return *sess.Config.Region
	}
	return ""
}
