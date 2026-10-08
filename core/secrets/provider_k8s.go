package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

func init() { Register("k8s", newK8sProvider, "kubernetes") }

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount/"

// k8sProvider reads a Kubernetes Secret:
// `ref+k8s://v1/Secret/<namespace>/<name>/<key>`. Without a key it returns
// all keys as a JSON object. In a cluster it calls the API with the service
// account token. Outside a cluster it runs kubectl.
type k8sProvider struct {
	cfg     ProviderConfig
	kubectl *cliRunner

	mu     sync.Mutex
	client *restClient
}

func newK8sProvider(cfg ProviderConfig) (Provider, error) {
	return &k8sProvider{
		cfg:     cfg,
		kubectl: newCLIRunner(cfg, "kubectl", "Install kubectl, or run sling in the cluster."),
	}, nil
}

func (p *k8sProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	parts := strings.Split(strings.Trim(ref.Path, "/"), "/")
	if len(parts) < 4 || len(parts) > 5 || parts[0] != "v1" || !strings.EqualFold(parts[1], "secret") {
		return nil, fmt.Errorf("use ref+k8s://v1/Secret/<namespace>/<name>/<key>")
	}
	ns, name, key := parts[2], parts[3], ""
	if len(parts) == 5 {
		key = parts[4]
	}

	var raw []byte
	var err error
	if host := p.host(ref); host != "" {
		raw, err = p.apiGet(ctx, host, ns, name)
	} else {
		raw, err = p.kubectlGet(ctx, ref, ns, name)
	}
	if err != nil {
		return nil, err
	}

	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(raw, &secret); err != nil {
		return nil, fmt.Errorf("could not decode secret %s/%s: %w", ns, name, err)
	}
	decoded := make(map[string]string, len(secret.Data))
	for k, v := range secret.Data {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("key %q of secret %s/%s is not base64", k, ns, name)
		}
		decoded[k] = string(b)
	}
	if key == "" {
		return json.Marshal(decoded)
	}
	v, ok := decoded[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in secret %s/%s", key, ns, name)
	}
	return []byte(v), nil
}

// host is the API server URL when sling runs in a cluster, else "".
func (p *k8sProvider) host(ref Ref) string {
	if h := p.cfg.Get("host"); h != "" {
		return h
	}
	if h := os.Getenv("KUBERNETES_SERVICE_HOST"); h != "" {
		port := os.Getenv("KUBERNETES_SERVICE_PORT")
		if port == "" {
			port = "443"
		}
		return "https://" + h + ":" + port
	}
	if _, ok := ref.Params["inCluster"]; ok {
		return "https://kubernetes.default.svc"
	}
	return ""
}

func (p *k8sProvider) apiGet(ctx context.Context, host, ns, name string) ([]byte, error) {
	tokenFile := p.cfg.Get("token_file")
	if tokenFile == "" {
		tokenFile = saDir + "token"
	}
	token, err := os.ReadFile(tokenFile) // read each time: the token rotates
	if err != nil {
		return nil, fmt.Errorf("could not read the service account token: %w", err)
	}

	p.mu.Lock()
	if p.client == nil {
		caFile := p.cfg.Get("ca_file")
		if caFile == "" {
			caFile = saDir + "ca.crt"
		}
		p.client, err = newRESTClient(p.cfg, host, restOptions{CACert: caFile})
	}
	client := p.client
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}

	path := "api/v1/namespaces/" + url.PathEscape(ns) + "/secrets/" + url.PathEscape(name)
	header := http.Header{"Authorization": {"Bearer " + strings.TrimSpace(string(token))}}
	return client.Do(ctx, http.MethodGet, path, nil, header, nil)
}

func (p *k8sProvider) kubectlGet(ctx context.Context, ref Ref, ns, name string) ([]byte, error) {
	args := []string{"get", "secret", name, "-n", ns, "-o", "json"}
	if kc := firstSet(ref.Params.Get("kubeConfigPath"), p.cfg.Get("kubeconfig")); kc != "" {
		args = append(args, "--kubeconfig", kc)
	}
	if c := firstSet(ref.Params.Get("kubeContext"), p.cfg.Get("context")); c != "" {
		args = append(args, "--context", c)
	}
	return p.kubectl.Run(ctx, nil, args...)
}

func firstSet(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
