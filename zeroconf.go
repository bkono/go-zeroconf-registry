package zeroconf

/*
	Zeroconf is a multicast dns registry for service discovery
	This creates a zero dependency system which is great
	where multicast dns is available. This usually depends
	on the ability to leverage udp and multicast/broadcast.
*/

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bkono/zeroconf"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/registry"
	hash "github.com/mitchellh/hashstructure"
)

type zeroconfTxt struct {
	Service   string
	Version   string
	Endpoints []*registry.Endpoint
	Metadata  map[string]string
}

type zeroconfEntry struct {
	hash uint64
	id   string
	node *zeroconf.Server
}

type zeroconfRegistry struct {
	opts registry.Options

	sync.Mutex
	services map[string][]*zeroconfEntry
}

func (z *zeroconfRegistry) Register(service *registry.Service, opts ...registry.RegisterOption) error {
	z.Lock()
	defer z.Unlock()
	log.Printf("in register, service = %+v\n", service)

	var reserr error

	entries, _ := z.services[service.Name]

	for _, node := range service.Nodes {
		h, err := hash.Hash(node, nil)
		if err != nil {
			log.Printf("failed getting hash for node, err = %+v\n", err)
			reserr = err
			continue
		}

		var seen bool
		var e *zeroconfEntry
		for _, entry := range entries {
			if node.Id == entry.id {
				seen = true
				e = entry
				break
			}
		}

		if seen && e.hash == h {
			// already registered
			log.Println("hash and entry already seen, continuing")
			continue
		} else if seen {
			// hash didn't match!
			log.Println("hash not matched, shutting down")
			e.node.Shutdown()
		} else {
			// wasn't found, new entry
			log.Println("new entry")
			e = &zeroconfEntry{hash: h}
		}

		txt, err := encode(&zeroconfTxt{
			Service:   service.Name,
			Version:   service.Version,
			Endpoints: service.Endpoints,
			Metadata:  node.Metadata,
		})
		if err != nil {
			log.Printf("err encoding, %+v\n", err)
			reserr = err
			continue
		}

		// made it all the way through, register the new node
		log.Printf("about to register, node.Id(%v) service.Name(%v) node.Port(%v) txt(%v)\n", node.Id, service.Name, node.Port, txt)
		srv, err := zeroconf.Register(node.Id, service.Name, "local.", node.Port, txt, nil)
		if err != nil {
			log.Printf("failed registering, err = %+v\n", err)
			reserr = err
			continue
		}

		e.id = node.Id
		e.node = srv
		entries = append(entries, e)
	}

	z.services[service.Name] = entries

	log.Printf("z.services[service.Name] = %+v\n", entries)

	return reserr
}

func (z *zeroconfRegistry) Deregister(service *registry.Service) error {
	z.Lock()
	defer z.Unlock()

	var newEntries []*zeroconfEntry
	// find and shutdown any existing entries that match
	for _, entry := range z.services[service.Name] {
		var remove bool
		for _, node := range service.Nodes {
			if node.Id == entry.id {
				entry.node.Shutdown()
				remove = true
				break
			}
		}

		if !remove {
			newEntries = append(newEntries, entry)
		}
	}

	z.services[service.Name] = newEntries
	return nil
}

func (z *zeroconfRegistry) GetService(service string) ([]*registry.Service, error) {
	log.Printf("in GetService(%v)\n", service)
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}

	entries := make(chan *zeroconf.ServiceEntry, 10)
	serviceMap := make(map[string]*registry.Service)

	go func(results <-chan *zeroconf.ServiceEntry) {
		for e := range results {
			log.Printf("e = %+v\n", e)
			if e.TTL == 0 {
				log.Println("skipping for 0 ttl")
				continue
			}

			txt, err := decode(e.Text)
			if err != nil || txt.Service != service || len(e.AddrIPv4) == 0 {
				log.Printf("skipping after decoding, e = %+v, txt = %+v\n", e, txt)
				continue
			}

			s, ok := serviceMap[txt.Version]
			if !ok {
				// first time seeing this version
				s = &registry.Service{
					Name:      txt.Service,
					Version:   txt.Version,
					Endpoints: txt.Endpoints,
				}
			}

			s.Nodes = append(s.Nodes, &registry.Node{
				Id:       e.Instance,
				Address:  e.AddrIPv4[0].String(),
				Port:     e.Port,
				Metadata: txt.Metadata,
			})
			serviceMap[txt.Version] = s
		}
	}(entries)

	log.Printf("setting timeout(%+v)\n", z.opts.Timeout)
	ctx, cancel := context.WithTimeout(context.Background(), z.opts.Timeout)
	defer cancel()
	err = resolver.Browse(ctx, service, "local.", entries)
	if err != nil {
		log.Printf("err during browse, err = %+v\n", err)
		return nil, err
	}
	log.Printf("browse complete\n")

	<-ctx.Done()

	var result []*registry.Service
	for _, service := range serviceMap {
		result = append(result, service)
	}

	return result, nil
}

func (z *zeroconfRegistry) ListServices() ([]*registry.Service, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}

	entries := make(chan *zeroconf.ServiceEntry, 10)
	serviceMap := make(map[string]bool)
	var services []*registry.Service

	go func(results <-chan *zeroconf.ServiceEntry) {
		for e := range results {
			if e.TTL == 0 {
				continue
			}

			name := strings.TrimSuffix(e.Instance, ".local.")
			if !serviceMap[name] {
				serviceMap[name] = true
				services = append(services, &registry.Service{Name: name})
			}
		}
	}(entries)

	ctx, cancel := context.WithTimeout(context.Background(), z.opts.Timeout)
	defer cancel()

	err = resolver.Browse(ctx, "_services._dns-sd._udp", "local.", entries)
	if err != nil {
		return nil, err
	}
	<-ctx.Done()

	return services, nil
}

func (z *zeroconfRegistry) Watch() (registry.Watcher, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &zeroconfWatcher{
		ch:     make(chan *zeroconf.ServiceEntry, 32),
		exit:   make(chan struct{}),
		cancel: cancel,
	}

	go func(resolver *zeroconf.Resolver, entries chan<- *zeroconf.ServiceEntry) {
		if err := resolver.Browse(ctx, "", "local.", entries); err != nil {
			w.Stop()
		}
	}(resolver, w.ch)

	return w, nil
}

func newRegistry(opts ...registry.Option) registry.Registry {
	options := registry.Options{
		Timeout: time.Millisecond * 100,
	}

	return &zeroconfRegistry{
		opts:     options,
		services: make(map[string][]*zeroconfEntry),
	}
}

func (z *zeroconfRegistry) String() string {
	return "zeroconf"
}

func NewRegistry(opts ...registry.Option) registry.Registry {
	return newRegistry(opts...)
}

func init() {
	cmd.DefaultRegistries["zeroconf"] = NewRegistry
}
