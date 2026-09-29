package secrets

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

func init() {
	Register("op", newOPProvider, "1password", "onepassword", "onepasswordconnect")
}

// opSDKFactory builds the 1Password SDK transport. It is set only in builds
// with the secrets_opsdk tag.
var opSDKFactory func(cfg ProviderConfig) (Provider, error)

const opMissingHint = `"op" is not installed. Install the 1Password CLI, or set OP_CONNECT_HOST for Connect.`

// opProvider reads 1Password secrets through the op CLI, Connect, or the SDK.
// The transport is chosen at the first fetch, not in the factory.
type opProvider struct {
	cfg ProviderConfig

	once      sync.Once
	transport Provider
	err       error
}

func newOPProvider(cfg ProviderConfig) (Provider, error) {
	return &opProvider{cfg: cfg}, nil
}

func (p *opProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	t, err := p.pick()
	if err != nil {
		return nil, err
	}
	return t.Get(ctx, ref)
}

func (p *opProvider) GetMany(ctx context.Context, refs []Ref) (map[string][]byte, error) {
	t, err := p.pick()
	if err != nil {
		return nil, err
	}
	if bg, ok := t.(batchGetter); ok {
		return bg.GetMany(ctx, refs)
	}
	out := map[string][]byte{}
	for _, ref := range refs {
		raw, err := t.Get(ctx, ref)
		if err != nil {
			return nil, err
		}
		out[ref.Key()] = raw
	}
	return out, nil
}

func (p *opProvider) Close() error {
	if c, ok := p.transport.(closer); ok {
		return c.Close()
	}
	return nil
}

// pick chooses the transport: `mode` if set; else CLI, Connect, SDK in order.
func (p *opProvider) pick() (Provider, error) {
	p.once.Do(func() {
		cli := p.newCLI()
		connectHost := p.cfg.Get("connect_host", "OP_CONNECT_HOST")
		switch mode := strings.ToLower(p.cfg.Get("mode")); mode {
		case "cli":
			p.transport = cli
		case "connect":
			p.transport, p.err = p.newConnect(connectHost)
		case "sdk":
			if opSDKFactory == nil {
				p.err = errors.New("the 1Password SDK is not in this build. Build with the secrets_opsdk tag, or use mode cli or connect")
				return
			}
			p.transport, p.err = opSDKFactory(p.cfg)
		case "":
			switch {
			case cli.runner.Installed():
				p.transport = cli
			case connectHost != "":
				p.transport, p.err = p.newConnect(connectHost)
			case opSDKFactory != nil:
				p.transport, p.err = opSDKFactory(p.cfg)
			default:
				p.err = errors.New(opMissingHint)
			}
		default:
			p.err = fmt.Errorf("unknown 1Password mode %q. Use cli, connect or sdk", mode)
		}
	})
	return p.transport, p.err
}

// opURI is the native op:// reference, with op query params kept.
func opURI(ref Ref) string {
	s := "op://" + ref.Path
	if q := ref.Params.Encode(); q != "" {
		s += "?" + q
	}
	return s
}

// opCLI runs `op read` and `op inject`.
type opCLI struct {
	runner  *cliRunner
	account string
}

func (p *opProvider) newCLI() *opCLI {
	runner := newCLIRunner(p.cfg, "op", "Install the 1Password CLI, or set OP_CONNECT_HOST for Connect.")
	runner.setEnv("OP_SERVICE_ACCOUNT_TOKEN", p.cfg.Get("token"))
	return &opCLI{runner: runner, account: p.cfg.Get("account", "OP_ACCOUNT")}
}

func (c *opCLI) args(args ...string) []string {
	if c.account != "" {
		args = append(args, "--account", c.account)
	}
	return args
}

func (c *opCLI) Get(ctx context.Context, ref Ref) ([]byte, error) {
	return c.runner.Run(ctx, nil, c.args("read", "--no-newline", opURI(ref))...)
}

// GetMany resolves all refs with one `op inject` call. Each value is put
// between unique markers, so multi-line values parse correctly.
func (c *opCLI) GetMany(ctx context.Context, refs []Ref) (map[string][]byte, error) {
	var tpl strings.Builder
	for i, ref := range refs {
		fmt.Fprintf(&tpl, "%s{{ %s }}%s\n", opMarker(i, "begin"), opURI(ref), opMarker(i, "end"))
	}
	out, err := c.runner.Run(ctx, []byte(tpl.String()), c.args("inject")...)
	if err != nil {
		return nil, err
	}
	text := string(out)
	got := map[string][]byte{}
	for i, ref := range refs {
		_, rest, ok := strings.Cut(text, opMarker(i, "begin"))
		if !ok {
			return nil, fmt.Errorf("op inject output has no value for %s", ref.Raw)
		}
		val, _, ok := strings.Cut(rest, opMarker(i, "end"))
		if !ok {
			return nil, fmt.Errorf("op inject output has no value for %s", ref.Raw)
		}
		got[ref.Key()] = []byte(val)
	}
	return got, nil
}

func opMarker(i int, side string) string {
	return fmt.Sprintf("<<<SLING-OP-%d-%s>>>", i, side)
}

// opConnect reads items from a 1Password Connect server.
type opConnect struct {
	rest *restClient
}

func (p *opProvider) newConnect(host string) (*opConnect, error) {
	if host == "" {
		return nil, errors.New("1Password Connect needs connect_host or OP_CONNECT_HOST")
	}
	token := p.cfg.Get("connect_token", "OP_CONNECT_TOKEN")
	if token == "" {
		return nil, errors.New("1Password Connect needs connect_token or OP_CONNECT_TOKEN")
	}
	rest, err := newRESTClient(p.cfg, host, restOptions{
		CACert:     p.cfg.Get("ca_cert"),
		SkipVerify: p.cfg.Bool("skip_verify"),
		Header:     map[string]string{"Authorization": "Bearer " + token},
	})
	if err != nil {
		return nil, err
	}
	return &opConnect{rest: rest}, nil
}

type opConnectItem struct {
	ID       string `json:"id"`
	Sections []struct {
		ID    string `json:"id"`
		Label string `json:"label"`
	} `json:"sections"`
	Fields []struct {
		ID      string `json:"id"`
		Label   string `json:"label"`
		Value   string `json:"value"`
		Section *struct {
			ID string `json:"id"`
		} `json:"section"`
	} `json:"fields"`
}

func (c *opConnect) Get(ctx context.Context, ref Ref) ([]byte, error) {
	parts := strings.Split(ref.Path, "/")
	if len(parts) < 3 || len(parts) > 4 {
		return nil, fmt.Errorf("a 1Password reference needs vault/item/[section/]field")
	}
	vaultName, itemName, field := parts[0], parts[1], parts[len(parts)-1]
	section := ""
	if len(parts) == 4 {
		section = parts[2]
	}

	vaultID, err := c.lookupID(ctx, "/v1/vaults", "name", vaultName)
	if err != nil {
		return nil, fmt.Errorf("vault %q: %w", vaultName, err)
	}
	itemID, err := c.lookupID(ctx, "/v1/vaults/"+url.PathEscape(vaultID)+"/items", "title", itemName)
	if err != nil {
		return nil, fmt.Errorf("item %q: %w", itemName, err)
	}

	var item opConnectItem
	path := "/v1/vaults/" + url.PathEscape(vaultID) + "/items/" + url.PathEscape(itemID)
	if _, err := c.rest.Do(ctx, http.MethodGet, path, nil, nil, &item); err != nil {
		return nil, err
	}

	sectionIDs := map[string]bool{}
	if section != "" {
		for _, s := range item.Sections {
			if s.ID == section || strings.EqualFold(s.Label, section) {
				sectionIDs[s.ID] = true
			}
		}
		if len(sectionIDs) == 0 {
			return nil, fmt.Errorf("section %q not found in item %q", section, itemName)
		}
	}
	for _, f := range item.Fields {
		if f.ID != field && !strings.EqualFold(f.Label, field) {
			continue
		}
		if section != "" && (f.Section == nil || !sectionIDs[f.Section.ID]) {
			continue
		}
		return []byte(f.Value), nil
	}
	return nil, fmt.Errorf("field %q not found in item %q", field, itemName)
}

// lookupID finds an ID by name with a Connect filter. When no name matches,
// the name is used as an ID.
func (c *opConnect) lookupID(ctx context.Context, path, attr, name string) (string, error) {
	q := url.Values{"filter": {fmt.Sprintf("%s eq %q", attr, name)}}
	var list []struct {
		ID string `json:"id"`
	}
	if _, err := c.rest.Do(ctx, http.MethodGet, path+"?"+q.Encode(), nil, nil, &list); err != nil {
		return "", err
	}
	switch len(list) {
	case 0:
		return name, nil
	case 1:
		return list[0].ID, nil
	}
	return "", fmt.Errorf("%d matches. Use the ID", len(list))
}
