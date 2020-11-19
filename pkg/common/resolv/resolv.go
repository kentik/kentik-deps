package resolv

import (
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/kentik/golog/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/naming"
)

/**
Implements basic resolver for gRPC calls.
*/

var (
	LOG_PREFIX = "[Resolve] "
)

type BasicResolver struct {
	log           *logger.Logger
	staticAddress string
}

func NewBasicResolver(staticAddress string, log *logger.Logger) *BasicResolver {
	return &BasicResolver{
		log:           log,
		staticAddress: staticAddress,
	}
}

// Allways just return the static address passed in at create time.
func (r *BasicResolver) Resolve(target string) (naming.Watcher, error) {
	return NewBasicWatcher(r.staticAddress, r.log)
}

// Watcher watches for the updates on the specified target.
type BasicWatcher struct {
	log     *logger.Logger
	end     chan bool
	targets []*naming.Update
	isFirst bool
}

// Next blocks until an update or error happens. It may return one or more
// updates. The first call should get the full set of the results. It should
// return an error if and only if Watcher cannot recover.
func (w *BasicWatcher) Next() ([]*naming.Update, error) {

	if w.isFirst {
		w.isFirst = false
		return w.targets, nil
	} else {
		for {
			select {
			case _ = <-w.end:
				w.log.Infof(LOG_PREFIX, "Watcher exiting")
				break
			}
		}
	}
}

// Close closes the Watcher.
func (w *BasicWatcher) Close() {
	w.end <- true
}

func NewBasicWatcher(targetRaw string, log *logger.Logger) (*BasicWatcher, error) {

	w := &BasicWatcher{
		log:     log,
		isFirst: true,
		end:     make(chan bool, 2),
		targets: make([]*naming.Update, 0),
	}

	targs := strings.Split(targetRaw, ",")
	if len(targs) == 0 {
		return nil, fmt.Errorf("Invalid target string: %s", targetRaw)
	} else {
		for _, t := range targs {
			w.targets = append(w.targets, &naming.Update{
				Op:       naming.Add,
				Addr:     t,
				Metadata: nil,
			})
		}
	}

	return w, nil
}

/**
Sets up a connecton using a set of authentication systems and returns the connection
*/
func GetClientConnection(address string, certUrl string, permFile string, log *logger.Logger) (*grpc.ClientConn, error) {
	// Pull from the url here
	if certUrl != "" {
		resp, err := http.Get(certUrl)
		if err != nil {
			return nil, err
		}
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()

		if resp.TLS != nil {
			cp := x509.NewCertPool()
			for _, c := range resp.TLS.PeerCertificates {
				cp.AddCert(c)
			}
			creds := credentials.NewClientTLSFromCert(cp, "")
			conn, err := grpc.Dial(address, grpc.WithTransportCredentials(creds), grpc.WithBlock(), grpc.WithBalancer(grpc.RoundRobin(NewBasicResolver(address, log))))
			if err != nil {
				return nil, err
			} else {
				return conn, nil
			}
		} else {
			return nil, fmt.Errorf("No TLS found")
		}
	} else if permFile != "" { // Or, load from file here
		creds, err := credentials.NewClientTLSFromFile(permFile, "")
		if err != nil {
			return nil, err
		}
		conn, err := grpc.Dial(address, grpc.WithTransportCredentials(creds), grpc.WithBlock(), grpc.WithBalancer(grpc.RoundRobin(NewBasicResolver(address, log))))
		if err != nil {
			return nil, err
		} else {
			return conn, nil
		}
	} else { // Or, fall back on no auth
		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithBalancer(grpc.RoundRobin(NewBasicResolver(address, log))))
		if err != nil {
			return nil, err
		} else {
			return conn, nil
		}
	}
}
