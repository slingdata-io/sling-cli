package api

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// Authenticate performs the auth workflow if needed. Like a Connect step.
// Header based auths (such as Basic, or Bearer) don't need this step
// save payload in APIState.Auth
func (ac *APIConnection) Authenticate() (err error) {

	if len(ac.Spec.Authentication) == 0 {
		ac.State.Auth.Authenticated = true
		return nil
	}

	authenticator, err := ac.MakeAuthenticator()
	if err != nil {
		return g.Error(err, "could not make authenticator for: %s", authenticator.AuthType())
	}

	if err = authenticator.Authenticate(ac.Context.Ctx, &ac.State.Auth); err != nil {
		return g.Error(err, "could not authenticate for: %s", authenticator.AuthType())
	}

	// set authenticated

	ac.State.Auth.Authenticated = true

	// Setup auth expiry timer
	if ac.Spec.Authentication.Expires() > 0 {
		// Calculate expiry time
		expiryDuration := time.Duration(ac.Spec.Authentication.Expires()) * time.Second
		ac.State.Auth.ExpiresAt = time.Now().Add(expiryDuration).Unix()
		g.Debug("authentication will expire at %s", time.Unix(ac.State.Auth.ExpiresAt, 0).Format(time.RFC3339))
	}

	for key, endpoint := range ac.Spec.EndpointMap {
		for k, v := range ac.State.Auth.Headers {
			if len(endpoint.Request.Headers) == 0 {
				endpoint.Request.Headers = g.M()
			}
			endpoint.Request.Headers[k] = v
		}
		ac.Spec.EndpointMap[key] = endpoint
	}

	return
}

type Authenticator interface {
	AuthType() AuthType
	Authenticate(ctx context.Context, state *APIStateAuth) error
	Refresh(ctx context.Context, state *APIStateAuth) error
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorBase
type AuthenticatorBase struct {
	Type    AuthType `yaml:"type" json:"type"`
	Expires int      `yaml:"expires" json:"expires,omitempty"` // when set, re-auth after number of seconds

	conn *APIConnection `yaml:"-" json:"-"`
}

func (a *AuthenticatorBase) AuthType() AuthType {
	return a.Type
}

func (a *AuthenticatorBase) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	return
}

func (a *AuthenticatorBase) Refresh(ctx context.Context, state *APIStateAuth) (err error) {
	return
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorBasic
type AuthenticatorBasic struct {
	AuthenticatorBase
	Username string `yaml:"username,omitempty" json:"username,omitempty"`
	Password string `yaml:"password,omitempty" json:"password,omitempty"`
}

func (a *AuthenticatorBasic) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	userPass, err := a.conn.renderString(g.F("%s:%s", a.Username, a.Password))
	if err != nil {
		return g.Error(err, "could not render user-password")
	}

	credentialsB64 := base64.StdEncoding.EncodeToString([]byte(userPass))
	state.Headers = map[string]string{"Authorization": g.F("Basic %s", credentialsB64)}

	return
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorSequence
type AuthenticatorSequence struct {
	AuthenticatorBase
	Sequence Sequence `yaml:"sequence" json:"sequence,omitempty"`
}

func (a *AuthenticatorSequence) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {

	baseEndpoint := &Endpoint{
		Name:    "api.auth",
		State:   g.M(),
		context: g.NewContext(a.conn.Context.Ctx),
		conn:    a.conn,
	}

	g.Debug("running authentication sequence")
	err = runSequence(a.Sequence, baseEndpoint)
	if err != nil {
		return g.Error(err, "could not auth via sequence")
	}

	// sync state back to APIConnection
	a.conn.Context.Lock()
	defer a.conn.Context.Unlock()

	// sync the state section back to ac.State.State
	if stateMap := baseEndpoint.State; len(stateMap) > 0 {
		for k, v := range stateMap {
			a.conn.State.State[k] = v
		}
	}

	return
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorOAuth2
type AuthenticatorOAuth2 struct {
	AuthenticatorBase
	Flow              OAuthFlow `yaml:"flow,omitempty" json:"flow,omitempty"`
	Username          string    `yaml:"username,omitempty" json:"username,omitempty"`
	Password          string    `yaml:"password,omitempty" json:"password,omitempty"`
	AuthenticationURL string    `yaml:"authentication_url,omitempty" json:"authentication_url,omitempty"` // Token endpoint
	AuthorizationURL  string    `yaml:"authorization_url,omitempty" json:"authorization_url,omitempty"`   // Authorization endpoint (for auth code flow)
	ClientID          string    `yaml:"client_id,omitempty" json:"client_id,omitempty"`
	ClientSecret      string    `yaml:"client_secret,omitempty" json:"client_secret,omitempty"`
	Token             string    `yaml:"token,omitempty" json:"token,omitempty"`
	Scopes            []string  `yaml:"scopes,omitempty" json:"scopes,omitempty"`
	RedirectURI       string    `yaml:"redirect_uri,omitempty" json:"redirect_uri,omitempty"`
	RefreshToken      string    `yaml:"refresh_token,omitempty" json:"refresh_token,omitempty"`
	RefreshOnExpire   bool      `yaml:"refresh_on_expire,omitempty" json:"refresh_on_expire,omitempty"`
}

func (a *AuthenticatorOAuth2) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	return
}

func (a *AuthenticatorOAuth2) Refresh(ctx context.Context, state *APIStateAuth) (err error) {
	return
}

// performOAuth2Flow handles different OAuth2 flows
func (a *AuthenticatorOAuth2) performOAuth2Flow() (token string, err error) {
	g.Debug("running OAuth2 flow (type=%#v)", a.Flow)
	switch a.Flow {
	case OAuthFlowClientCredentials:
		return a.performClientCredentialsFlow()
	case OAuthFlowRefreshToken:
		return a.performRefreshTokenFlow()
	case OAuthFlowPassword:
		return a.performPasswordFlow()
	case OAuthFlowAuthorizationCode:
		return a.performAuthorizationCodeFlow()
	default:
		return "", g.Error("unsupported OAuth2 flow: %s", a.Flow)
	}
}

// performClientCredentialsFlow implements the OAuth2 Client Credentials flow
func (a *AuthenticatorOAuth2) performClientCredentialsFlow() (token string, err error) {

	// Render template values
	clientID, err := a.conn.renderString(a.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := a.conn.renderString(a.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	authURL, err := a.conn.renderString(a.AuthenticationURL)
	if err != nil {
		return "", g.Error(err, "could not render authentication_url")
	}

	if clientID == "" {
		return "", g.Error("client_id is required for client_credentials flow")
	}
	if clientSecret == "" {
		return "", g.Error("client_secret is required for client_credentials flow")
	}
	if authURL == "" {
		return "", g.Error("authentication_url is required for client_credentials flow")
	}

	// Prepare request payload
	payload := map[string]string{
		"grant_type":    "client_credentials",
		"client_id":     clientID,
		"client_secret": clientSecret,
	}

	// Add scopes if provided
	if len(a.Scopes) > 0 {
		payload["scope"] = strings.Join(a.Scopes, " ")
	}

	// Make request
	resp, err := a.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 response")
}

// performRefreshTokenFlow implements the OAuth2 Refresh Token flow
func (a *AuthenticatorOAuth2) performRefreshTokenFlow() (token string, err error) {

	// Render template values
	clientID, err := a.conn.renderString(a.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := a.conn.renderString(a.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	refreshToken, err := a.conn.renderString(a.RefreshToken)
	if err != nil {
		return "", g.Error(err, "could not render refresh_token")
	}

	authURL, err := a.conn.renderString(a.AuthenticationURL)
	if err != nil {
		return "", g.Error(err, "could not render authentication_url")
	}

	if refreshToken == "" {
		return "", g.Error("refresh_token is required for refresh_token flow")
	}
	if authURL == "" {
		return "", g.Error("authentication_url is required for refresh_token flow")
	}

	// Prepare request payload
	payload := map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": refreshToken,
	}

	// Add client credentials if provided
	if clientID != "" {
		payload["client_id"] = clientID
	}
	if clientSecret != "" {
		payload["client_secret"] = clientSecret
	}

	// Make request
	resp, err := a.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 refresh request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		// Update refresh token if a new one was provided
		if newRefreshToken, ok := resp["refresh_token"].(string); ok {
			a.conn.State.Auth.Token = newRefreshToken
		}
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 refresh response")
}

// performPasswordFlow implements the OAuth2 Resource Owner Password Credentials flow
func (a *AuthenticatorOAuth2) performPasswordFlow() (token string, err error) {
	// Render template values
	clientID, err := a.conn.renderString(a.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := a.conn.renderString(a.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	username, err := a.conn.renderString(a.Username)
	if err != nil {
		return "", g.Error(err, "could not render username")
	}

	password, err := a.conn.renderString(a.Password)
	if err != nil {
		return "", g.Error(err, "could not render password")
	}

	authURL, err := a.conn.renderString(a.AuthenticationURL)
	if err != nil {
		return "", g.Error(err, "could not render authentication_url")
	}

	if username == "" {
		return "", g.Error("username is required for password flow")
	}
	if password == "" {
		return "", g.Error("password is required for password flow")
	}
	if authURL == "" {
		return "", g.Error("authentication_url is required for password flow")
	}

	// Prepare request payload
	payload := map[string]string{
		"grant_type": "password",
		"username":   username,
		"password":   password,
	}

	// Add client credentials if provided
	if clientID != "" {
		payload["client_id"] = clientID
	}
	if clientSecret != "" {
		payload["client_secret"] = clientSecret
	}

	// Add scopes if provided
	if len(a.Scopes) > 0 {
		payload["scope"] = strings.Join(a.Scopes, " ")
	}

	// Make request
	resp, err := a.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 password request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		// Store refresh token if provided
		if refreshToken, ok := resp["refresh_token"].(string); ok {
			a.conn.State.Auth.Token = refreshToken
		}
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 password response")
}

// performAuthorizationCodeFlow implements the OAuth2 Authorization Code flow
func (a *AuthenticatorOAuth2) performAuthorizationCodeFlow() (token string, err error) {
	// we already have refresh token
	if a.RefreshToken != "" {
		return a.performRefreshTokenFlow()
	}

	// Check if we should use the interactive server flow
	if a.RedirectURI == "" || strings.Contains(a.RedirectURI, "localhost") || strings.Contains(a.RedirectURI, "127.0.0.1") {
		return a.performAuthorizationCodeFlowWithServer()
	}

	// This flow assumes the authorization code is already provided in the Token field
	authCode, err := a.conn.renderString(a.Token)
	if err != nil {
		return "", g.Error(err, "could not render authorization code")
	}

	if authCode == "" {
		return "", g.Error("authorization code is required for authorization_code flow (provide in token field)")
	}

	return a.exchangeCodeForToken(authCode)
}

// performAuthorizationCodeFlowWithServer implements the OAuth2 Authorization Code flow with a local server
func (a *AuthenticatorOAuth2) performAuthorizationCodeFlowWithServer() (token string, err error) {
	// Render template values
	clientID, err := a.conn.renderString(a.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := a.conn.renderString(a.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	authURL, err := a.conn.renderString(a.AuthenticationURL)
	if err != nil {
		return "", g.Error(err, "could not render authentication_url")
	}

	if clientID == "" {
		return "", g.Error("client_id is required for authorization_code flow")
	}
	if clientSecret == "" {
		return "", g.Error("client_secret is required for authorization_code flow")
	}
	if authURL == "" {
		return "", g.Error("authentication_url is required for authorization_code flow")
	}

	// Find an available port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return "", g.Error(err, "failed to find available port")
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Set up the redirect URI
	a.RedirectURI = fmt.Sprintf("http://localhost:%d/callback", port)

	// Create channels for communication
	codeChan := make(chan string, 1)
	errorChan := make(chan error, 1)

	// Set up HTTP server
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			// Shutdown server after handling callback
			go func() {
				time.Sleep(1 * time.Second) // Give time for response to be sent
				server.Shutdown(context.Background())
			}()
		}()

		// Check for error in callback
		if errorCode := r.URL.Query().Get("error"); errorCode != "" {
			errorDesc := r.URL.Query().Get("error_description")
			if errorDesc == "" {
				errorDesc = errorCode
			}

			fmt.Fprintf(w, `
<!DOCTYPE html>
<html>
<head><title>Authorization Failed</title></head>
<body>
<h1>Authorization Failed</h1>
<p>Error: %s</p>
<p>You can close this window.</p>
</body>
</html>`, errorDesc)

			errorChan <- g.Error("OAuth2 authorization failed: %s", errorDesc)
			return
		}

		// Get authorization code
		code := r.URL.Query().Get("code")
		if code == "" {
			fmt.Fprintf(w, `
<!DOCTYPE html>
<html>
<head><title>Authorization Failed</title></head>
<body>
<h1>Authorization Failed</h1>
<p>No authorization code received.</p>
<p>You can close this window.</p>
</body>
</html>`)
			errorChan <- g.Error("no authorization code received")
			return
		}

		// Success response
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html>
<head><title>Authorization Successful</title></head>
<body>
<h1>Authorization Successful!</h1>
<p>You can close this window and return to the application.</p>
<script>
// Auto-close after 3 seconds
setTimeout(function() {
    window.close();
}, 3000);
</script>
</body>
</html>`)

		codeChan <- code
	})

	// Start the server
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errorChan <- g.Error(err, "failed to start callback server")
		}
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Build authorization URL
	params := url.Values{}
	params.Set("response_type", "code")
	params.Set("client_id", clientID)
	params.Set("redirect_uri", a.RedirectURI)
	if len(a.Scopes) > 0 {
		params.Set("scope", strings.Join(a.Scopes, " "))
	}
	// Add a state parameter for security (optional but recommended)
	params.Set("state", fmt.Sprintf("sling-%d", time.Now().Unix()))

	// Determine the authorization endpoint (different from token endpoint)
	// First check if authorization_url is explicitly provided
	authorizeURL, err := a.conn.renderString(a.AuthorizationURL)
	if err != nil {
		return "", g.Error(err, "could not render authorization_url")
	}

	// If not provided, derive from token endpoint URL
	if authorizeURL == "" {
		// Most OAuth2 providers have /authorize for auth and /token for token exchange
		authorizeURL = strings.Replace(authURL, "/token", "/authorize", 1)
		if authorizeURL == authURL {
			// If no replacement happened, try common patterns
			if strings.HasSuffix(authURL, "/oauth/token") {
				authorizeURL = strings.Replace(authURL, "/oauth/token", "/oauth/authorize", 1)
			} else if strings.HasSuffix(authURL, "/token") {
				authorizeURL = strings.Replace(authURL, "/token", "/authorize", 1)
			} else {
				// Fallback: append /authorize to the base URL
				baseURL := strings.TrimSuffix(authURL, "/")
				authorizeURL = baseURL + "/authorize"
			}
		}
	}

	fullAuthURL := fmt.Sprintf("%s?%s", authorizeURL, params.Encode())

	// Open browser
	g.Info("Opening browser for OAuth2 authorization...")
	g.Info("If the browser doesn't open automatically, please visit: %s", fullAuthURL)

	if err := a.openBrowser(fullAuthURL); err != nil {
		g.Warn("Failed to open browser automatically: %v", err)
		g.Info("Please manually open the following URL in your browser:")
		g.Info("%s", fullAuthURL)
	}

	// Wait for authorization code or timeout
	select {
	case code := <-codeChan:
		g.Info("Authorization code received, exchanging for access token...")
		return a.exchangeCodeForToken(code)
	case err := <-errorChan:
		return "", err
	case <-time.After(5 * time.Minute):
		server.Shutdown(context.Background())
		return "", g.Error("authorization timeout after 5 minutes")
	case <-a.conn.Context.Ctx.Done():
		server.Shutdown(context.Background())
		return "", g.Error("authorization cancelled")
	}
}

// exchangeCodeForToken exchanges an authorization code for an access token
func (a *AuthenticatorOAuth2) exchangeCodeForToken(code string) (token string, err error) {
	// Render template values
	clientID, err := a.conn.renderString(a.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := a.conn.renderString(a.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	redirectURI, err := a.conn.renderString(a.RedirectURI)
	if err != nil {
		return "", g.Error(err, "could not render redirect_uri")
	}

	authURL, err := a.conn.renderString(a.AuthenticationURL)
	if err != nil {
		return "", g.Error(err, "could not render authentication_url")
	}

	if clientID == "" {
		return "", g.Error("client_id is required for authorization_code flow")
	}
	if clientSecret == "" {
		return "", g.Error("client_secret is required for authorization_code flow")
	}
	if authURL == "" {
		return "", g.Error("authentication_url is required for authorization_code flow")
	}

	// For server flow, use the localhost redirect URI
	if redirectURI == "" {
		redirectURI = "http://localhost:8080/callback"
	}

	// Prepare request payload
	payload := map[string]string{
		"grant_type":    "authorization_code",
		"code":          code,
		"client_id":     clientID,
		"client_secret": clientSecret,
		"redirect_uri":  redirectURI,
	}

	// Make request
	resp, err := a.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 authorization code request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		// Store refresh token if provided
		if refreshToken, ok := resp["refresh_token"].(string); ok {
			// TODO: need to be stored in home folder
			g.Debug("OAuth refreshToken = %s", refreshToken)
			a.conn.State.Auth.Token = refreshToken
		}
		g.Trace("OAuth accessToken = %s", accessToken)
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 authorization code response")
}

// makeOAuthRequest makes an HTTP request for OAuth2 token exchange
func (a *AuthenticatorOAuth2) makeOAuthRequest(url string, payload map[string]string) (response map[string]any, err error) {
	// Convert payload to form data
	formData := strings.Builder{}
	first := true
	for key, value := range payload {
		if !first {
			formData.WriteString("&")
		}
		formData.WriteString(key)
		formData.WriteString("=")
		formData.WriteString(strings.ReplaceAll(value, " ", "%20"))
		first = false
	}

	// Create HTTP request
	a.conn.Context.Debug("oauth POST request to %s", url)
	a.conn.Context.Trace("oauth POST request payload: %s", g.Marshal(payload))
	a.conn.Context.Trace("oauth POST request form data: %s", formData.String())
	req, err := http.NewRequestWithContext(a.conn.Context.Ctx, "POST", url, strings.NewReader(formData.String()))
	if err != nil {
		return nil, g.Error(err, "failed to create OAuth2 request")
	}

	// Set headers
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	// Make the request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, g.Error(err, "failed to execute OAuth2 request")
	}

	a.conn.Context.Debug("oauth POST response status code: %d", resp.StatusCode)

	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, g.Error(err, "failed to read OAuth2 response body")
	}

	a.conn.Context.Trace("oauth POST response body: %s", string(body))

	// Check for HTTP errors
	if resp.StatusCode >= 400 {
		return nil, g.Error("OAuth2 request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse JSON response
	var result map[string]any
	if err := g.JSONUnmarshal(body, &result); err != nil {
		return nil, g.Error(err, "failed to parse OAuth2 response JSON")
	}

	// Check for OAuth2 error in response
	if errorCode, ok := result["error"].(string); ok {
		errorDesc := ""
		if desc, ok := result["error_description"].(string); ok {
			errorDesc = ": " + desc
		}
		return nil, g.Error("OAuth2 error: %s%s", errorCode, errorDesc)
	}

	return result, nil
}

// openBrowser opens the default browser to the given URL
func (a *AuthenticatorOAuth2) openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...).Start()
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorAWSSigV4
type AuthenticatorAWSSigV4 struct {
	AuthenticatorBase
	AwsService         string `yaml:"aws_service,omitempty" json:"aws_service,omitempty"`
	AwsAccessKeyID     string `yaml:"aws_access_key_id,omitempty" json:"aws_access_key_id,omitempty"`
	AwsSecretAccessKey string `yaml:"aws_secret_access_key,omitempty" json:"aws_secret_access_key,omitempty"`
	AwsSessionToken    string `yaml:"aws_session_token,omitempty" json:"aws_session_token,omitempty"`
	AwsRegion          string `yaml:"aws_region,omitempty" json:"aws_region,omitempty"`
	AwsProfile         string `yaml:"aws_profile,omitempty" json:"aws_profile,omitempty"`
}

func (a *AuthenticatorAWSSigV4) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	ac := a.conn

	props := map[string]string{
		"aws_service":           a.AwsService,
		"aws_access_key_id":     a.AwsAccessKeyID,
		"aws_secret_access_key": a.AwsSecretAccessKey,
		"aws_session_token":     a.AwsSessionToken,
		"aws_region":            a.AwsRegion,
		"aws_profile":           a.AwsProfile,
	}

	// render prop values
	for key, val := range props {
		props[key], err = ac.renderString(val)
		if err != nil {
			return g.Error(err, "could not render %s", key)
		}
	}

	// get aws service and region
	awsService := cast.ToString(props["aws_service"])
	awsRegion := cast.ToString(props["aws_region"])

	if awsRegion == "" {
		return g.Error(err, "did not provide aws_region")
	}
	if awsService == "" {
		return g.Error(err, "did not provide aws_service")
	}

	// load AWS creds
	cfg, err := iop.MakeAwsConfig(ac.Context.Ctx, props)
	if err != nil {
		return g.Error(err, "could not make AWS config for authentication")
	}

	ac.State.Auth.Sign = func(ctx context.Context, req *http.Request, bodyBytes []byte) error {
		// Calculate the SHA256 hash of the request body.
		hasher := sha256.New()
		hasher.Write(bodyBytes)
		payloadHash := hex.EncodeToString(hasher.Sum(nil))

		// Create a new signer.
		signer := v4.NewSigner()

		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return g.Error(err, "could not retrieve AWS creds signing request")
		}

		// Sign the request, which adds the 'Authorization' and other necessary headers.
		return signer.SignHTTP(ctx, creds, req, payloadHash, awsService, awsRegion, time.Now())
	}

	return
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorHMAC
type AuthenticatorHMAC struct {
	AuthenticatorBase
	Algorithm     string            `yaml:"algorithm" json:"algorithm"`
	Secret        string            `yaml:"secret" json:"secret"`
	SigningString string            `yaml:"signing_string" json:"signing_string"`
	ReqHeaders    map[string]string `yaml:"request_headers" json:"request_headers"`
	NonceLength   int               `yaml:"nonce_length" json:"nonce_length,omitempty"` // Length of random nonce bytes (0 to disable)
}

func (a *AuthenticatorHMAC) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	if a.Algorithm == "" {
		a.Algorithm = "sha256"
	}

	state.Sign = func(ctx context.Context, req *http.Request, bodyBytes []byte) error {
		var signature string

		// Fixed timestamp for consistency
		now := time.Now().UTC()
		timestampSec := now.Unix()
		timestampMs := now.UnixMilli()

		// Path includes query params
		path := req.URL.RequestURI()

		// Compute body hashes for various algorithms
		bodyHashMd5 := md5.Sum(bodyBytes)
		bodyHashMd5Hex := hex.EncodeToString(bodyHashMd5[:])

		bodyHashSha1 := sha1.Sum(bodyBytes)
		bodyHashSha1Hex := hex.EncodeToString(bodyHashSha1[:])

		bodyHashSha256 := sha256.Sum256(bodyBytes)
		bodyHashSha256Hex := hex.EncodeToString(bodyHashSha256[:])

		bodyHashSha512 := sha512.Sum512(bodyBytes)
		bodyHashSha512Hex := hex.EncodeToString(bodyHashSha512[:])

		// Raw body
		bodyRaw := string(bodyBytes)

		// Canonical query string (sorted)
		queryValues, _ := url.ParseQuery(req.URL.RawQuery)
		var queryParts []string
		var queryKeys []string
		for k := range queryValues {
			queryKeys = append(queryKeys, k)
		}
		sort.Strings(queryKeys)
		for _, k := range queryKeys {
			vals := queryValues[k]
			sort.Strings(vals)
			for _, v := range vals {
				queryParts = append(queryParts, url.QueryEscape(k)+"="+url.QueryEscape(v))
			}
		}
		canonicalQuery := strings.Join(queryParts, "&")

		// Canonical headers (lowercase keys, sorted, joined values)
		var headersStr strings.Builder
		headerKeys := make([]string, 0, len(req.Header))
		for k := range req.Header {
			headerKeys = append(headerKeys, strings.ToLower(k))
		}
		sort.Strings(headerKeys)
		for _, k := range headerKeys {
			origK := req.Header[k] // Note: keys are case-insensitive, but we use lowered for sorting
			headersStr.WriteString(k + ":" + strings.Join(origK, ",") + "\n")
		}
		canonicalHeaders := headersStr.String()

		// Date formats
		dateIso := now.Format(time.RFC3339)
		dateRfc1123 := now.Format(time.RFC1123)

		// Generate nonce if enabled
		var nonce string
		if a.NonceLength > 0 {
			nonceBytes := make([]byte, a.NonceLength)
			if _, err := rand.Read(nonceBytes); err != nil {
				return g.Error(err, "could not generate nonce")
			}
			nonce = hex.EncodeToString(nonceBytes)
		}

		templateMap := g.M(
			"http_method", req.Method,
			"http_path", path,
			"http_body_md5", bodyHashMd5Hex,
			"http_body_sha1", bodyHashSha1Hex,
			"http_body_sha256", bodyHashSha256Hex,
			"http_body_sha512", bodyHashSha512Hex,
			"http_body_raw", bodyRaw,
			"http_query", canonicalQuery,
			"http_headers", canonicalHeaders,
			"unix_time", cast.ToString(timestampSec),
			"unix_time_ms", cast.ToString(timestampMs),
			"date_iso", dateIso,
			"date_rfc1123", dateRfc1123,
			"nonce", nonce,
		)

		switch a.Algorithm {
		case "sha256":
			stringToSign := g.Rm(a.SigningString, templateMap)

			stringToSign, err = a.conn.renderString(stringToSign)
			if err != nil {
				return g.Error(err, "could not render string for HMAC signer")
			}

			// Compute HMAC-SHA256
			mac := hmac.New(sha256.New, []byte(a.Secret))
			mac.Write([]byte(stringToSign))
			signature = hex.EncodeToString(mac.Sum(nil))
		case "sha512":
			stringToSign := g.Rm(a.SigningString, templateMap)

			stringToSign, err = a.conn.renderString(stringToSign)
			if err != nil {
				return g.Error(err, "could not render string for HMAC signer")
			}

			// Compute HMAC-SHA512
			mac := hmac.New(sha512.New, []byte(a.Secret))
			mac.Write([]byte(stringToSign))
			signature = hex.EncodeToString(mac.Sum(nil))
		default:
			return g.Error("invalid algorithm (%s), only 'sha256' and 'sha512' are supported", a.Algorithm)
		}

		// set signature
		templateMap["signature"] = signature

		// Add headers
		for key, value := range a.ReqHeaders {
			value = g.Rm(value, templateMap)
			value, err = a.conn.renderString(value)
			if err != nil {
				return g.Error(err, "could not render string for HMAC header: %s", key)
			}
			req.Header.Set(key, value)
		}

		return nil
	}

	return
}
