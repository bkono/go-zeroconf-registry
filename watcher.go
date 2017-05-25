package zeroconf

import (
	"context"
	"errors"
	"strings"

	"github.com/bkono/zeroconf"
	"github.com/micro/go-micro/registry"
)

type zeroconfWatcher struct {
	ch     chan *zeroconf.ServiceEntry
	exit   chan struct{}
	cancel context.CancelFunc
}

func (z *zeroconfWatcher) Next() (*registry.Result, error) {
	for {
		select {
		case e := <-z.ch:
			txt, err := decode(e.Text)
			if err != nil {
				continue
			}

			if len(txt.Service) == 0 || len(txt.Version) == 0 || len(e.AddrIPv4) == 0 {
				continue
			}

			var action string

			if e.TTL == 0 {
				action = "delete"
			} else {
				action = "create"
			}

			service := &registry.Service{
				Name:      txt.Service,
				Version:   txt.Version,
				Endpoints: txt.Endpoints,
			}

			// TODO: don't hardcode .local.
			zn := CleanServiceName(service.Name) + ".local."
			if !strings.HasSuffix(e.Instance, zn) {
				continue
			}

			service.Nodes = append(service.Nodes, &registry.Node{
				Id:       strings.TrimSuffix(e.Instance, "."+service.Name+".local."),
				Address:  e.AddrIPv4[0].String(),
				Port:     e.Port,
				Metadata: txt.Metadata,
			})

			return &registry.Result{
				Action:  action,
				Service: service,
			}, nil
		case <-z.exit:
			return nil, errors.New("watcher stopped")
		}
	}
}

func (z *zeroconfWatcher) Stop() {
	select {
	case <-z.exit:
		return
	default:
		z.cancel()
		close(z.exit)
	}
}
