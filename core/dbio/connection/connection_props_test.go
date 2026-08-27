package connection

import (
	"strings"
	"testing"
)

func TestRejectLiteralSecretsNested(t *testing.T) {
	err := RejectLiteralSecrets("MY_API", map[string]any{
		"type": "api",
		"spec": "stripe",
		"secrets": map[string]any{
			"api_key": "sk_live_distinctive",
		},
	})
	if err == nil {
		t.Fatal("expected literal nested secret to be refused")
	}
	if !strings.Contains(err.Error(), "secrets.api_key") && !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("error should name the field: %v", err)
	}
}
