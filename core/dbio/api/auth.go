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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
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

	return
}

func (ac *APIConnection) MakeAuthenticator() (authenticator Authenticator, err error) {

	baseAuth := AuthenticatorBase{Type: ac.Spec.Authentication.Type(), conn: ac}

	switch baseAuth.Type {
	case AuthTypeNone:
		authenticator = &baseAuth
	case AuthTypeStatic:
		authenticator = &AuthenticatorStatic{AuthenticatorBase: baseAuth}
	case AuthTypeBasic:
		authenticator = &AuthenticatorBasic{AuthenticatorBase: baseAuth}
	case AuthTypeSequence:
		authenticator = &AuthenticatorSequence{AuthenticatorBase: baseAuth}
	case AuthTypeOAuth2:
		authenticator = &AuthenticatorOAuth2{AuthenticatorBase: baseAuth}
	case AuthTypeAWSSigV4:
		authenticator = &AuthenticatorAWSSigV4{AuthenticatorBase: baseAuth}
	case AuthTypeHMAC:
		authenticator = &AuthenticatorHMAC{AuthenticatorBase: baseAuth}
	}

	// so we write all the properties
	if err = g.JSONConvert(ac.Spec.Authentication, authenticator); err != nil {
		return nil, g.Error(err, "could not make authenticator for: %s", baseAuth.Type)
	}

	return
}

// IsAuthExpired checks if the authentication has expired
func (ac *APIConnection) IsAuthExpired() bool {
	if ac.State.Auth.ExpiresAt == 0 {
		return false // No expiry set
	}
	return time.Now().Unix() >= ac.State.Auth.ExpiresAt
}

// EnsureAuthenticated checks if authentication is valid and re-authenticates if needed
// This method ensures thread-safe authentication checks and re-authentication
func (ac *APIConnection) EnsureAuthenticated() error {
	ac.State.Auth.Mutex.Lock()
	defer ac.State.Auth.Mutex.Unlock()

	if ac.Spec.Authentication == nil {
		ac.State.Auth.Authenticated = true
		return nil
	}

	// Check if authentication has expired or not authenticated
	if !ac.State.Auth.Authenticated || ac.IsAuthExpired() {
		g.Trace("authentication expired or not authenticated, re-authenticating...")
		if err := ac.Authenticate(); err != nil {
			return g.Error(err, "failed to authenticate")
		}
	}
	return nil
}

///////////////////////////////////////////////

// Authenticate performs the auth workflow if needed. Like a Connect step.
// Header based auths (such as Basic, or Bearer) don't need this step
// save payload in APIState.Auth
func (ep *Endpoint) Authenticate() (err error) {

	if ep.Authentication == nil {
		ep.auth.Authenticated = true
		return nil
	}

	authenticator, err := ep.MakeAuthenticator()
	if err != nil {
		return g.Error(err, "could not make authenticator for: %s", authenticator.AuthType())
	}

	if err = authenticator.Authenticate(ep.context.Ctx, &ep.auth); err != nil {
		return g.Error(err, "could not authenticate for: %s", authenticator.AuthType())
	}

	// set authenticated
	ep.auth.Authenticated = true

	// Setup auth expiry timer
	if ep.Authentication.Expires() > 0 {
		// Calculate expiry time
		expiryDuration := time.Duration(ep.Authentication.Expires()) * time.Second
		ep.auth.ExpiresAt = time.Now().Add(expiryDuration).Unix()
		g.Debug("authentication will expire at %s", time.Unix(ep.auth.ExpiresAt, 0).Format(time.RFC3339))
	}

	return
}

func (ep *Endpoint) specifiesAuthentication() bool {
	_, exists := ep.originalMap["authentication"]
	return exists
}

func (ep *Endpoint) EnsureAuthenticated() error {
	ep.auth.Mutex.Lock()
	defer ep.auth.Mutex.Unlock()

	if ep.Authentication == nil {
		// if specified as null, no auth required
		if ep.specifiesAuthentication() {
			ep.auth.Authenticated = true
			return nil
		}

		return ep.conn.EnsureAuthenticated()
	}

	// Check if authentication has expired or not authenticated
	if !ep.auth.Authenticated || ep.IsAuthExpired() {
		g.Trace("authentication expired or not authenticated, re-authenticating...")
		if err := ep.Authenticate(); err != nil {
			return g.Error(err, "failed to authenticate")
		}
	}
	return nil
}

// IsAuthExpired checks if the authentication has expired
func (ep *Endpoint) IsAuthExpired() bool {
	if ep.auth.ExpiresAt == 0 {
		return false // No expiry set
	}
	return time.Now().Unix() >= ep.auth.ExpiresAt
}

func (ep *Endpoint) MakeAuthenticator() (authenticator Authenticator, err error) {

	baseAuth := AuthenticatorBase{
		Type:     ep.Authentication.Type(),
		conn:     ep.conn,
		endpoint: ep,
	}

	switch baseAuth.Type {
	case AuthTypeNone:
		authenticator = &baseAuth
	case AuthTypeStatic:
		authenticator = &AuthenticatorStatic{AuthenticatorBase: baseAuth}
	case AuthTypeBasic:
		authenticator = &AuthenticatorBasic{AuthenticatorBase: baseAuth}
	case AuthTypeSequence:
		authenticator = &AuthenticatorSequence{AuthenticatorBase: baseAuth}
	case AuthTypeOAuth2:
		authenticator = &AuthenticatorOAuth2{AuthenticatorBase: baseAuth}
	case AuthTypeAWSSigV4:
		authenticator = &AuthenticatorAWSSigV4{AuthenticatorBase: baseAuth}
	case AuthTypeHMAC:
		authenticator = &AuthenticatorHMAC{AuthenticatorBase: baseAuth}
	}

	// so we write all the properties
	if err = g.JSONConvert(ep.Authentication, authenticator); err != nil {
		return nil, g.Error(err, "could not make authenticator for: %s", baseAuth.Type)
	}

	return
}

////////////////////////////////////////////

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

	conn     *APIConnection `yaml:"-" json:"-"`
	endpoint *Endpoint      `yaml:"-" json:"-"`
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

func (a *AuthenticatorBase) renderString(val any, extraMaps ...map[string]any) (string, error) {
	if a.endpoint != nil {
		return a.endpoint.renderString(val, extraMaps...)
	}
	return a.conn.renderString(val, extraMaps...)
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorBasic
type AuthenticatorBasic struct {
	AuthenticatorBase
	Username string `yaml:"username,omitempty" json:"username,omitempty"`
	Password string `yaml:"password,omitempty" json:"password,omitempty"`
}

func (a *AuthenticatorBasic) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	userPass, err := a.renderString(g.F("%s:%s", a.Username, a.Password))
	if err != nil {
		return g.Error(err, "could not render user-password")
	}

	credentialsB64 := base64.StdEncoding.EncodeToString([]byte(userPass))
	state.Headers = map[string]string{"Authorization": g.F("Basic %s", credentialsB64)}

	return
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorStatic
type AuthenticatorStatic struct {
	AuthenticatorBase
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
}

func (a *AuthenticatorStatic) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	// Initialize headers map
	state.Headers = make(map[string]string)

	// Render each header value (supports templating like {secrets.API_KEY})
	for headerName, headerValue := range a.Headers {
		renderedVal, err := a.renderString(headerValue)
		if err != nil {
			return g.Error(err, "could not render header value for: %s", headerName)
		}
		state.Headers[headerName] = renderedVal
	}

	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////
// AuthenticatorSequence
type AuthenticatorSequence struct {
	AuthenticatorBase
	Sequence Sequence `yaml:"sequence" json:"sequence,omitempty"`
}

func (a *AuthenticatorSequence) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {

	baseEndpoint := &Endpoint{
		Name:      "api.auth",
		State:     g.M(),
		context:   g.NewContext(a.conn.Context.Ctx),
		conn:      a.conn,
		aggregate: NewAggregateState(),
		auth:      APIStateAuth{Mutex: &sync.Mutex{}},
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
	ClientID          string    `yaml:"client_id,omitempty" json:"client_id,omitempty"`
	ClientSecret      string    `yaml:"client_secret,omitempty" json:"client_secret,omitempty"`
	RedirectURI       string    `yaml:"redirect_uri,omitempty" json:"redirect_uri,omitempty"`
	AuthenticationURL string    `yaml:"authentication_url,omitempty" json:"authentication_url,omitempty"` // Token endpoint
	AuthorizationURL  string    `yaml:"authorization_url,omitempty" json:"authorization_url,omitempty"`   // Auth endpoint
	DeviceAuthURL     string    `yaml:"device_auth_url,omitempty" json:"device_auth_url,omitempty"`       // Device auth endpoint (optional)
	Scopes            []string  `yaml:"scopes,omitempty" json:"scopes,omitempty"`
	Flow              OAuthFlow `yaml:"flow,omitempty" json:"flow,omitempty"` // e.g., "authorization_code", "client_credentials", "device_code"
}

// Authenticate performs OAuth2 authentication based on the configured flow.
func (a *AuthenticatorOAuth2) Authenticate(ctx context.Context, state *APIStateAuth) (err error) {
	// Render template values
	clientID, err := a.renderString(a.ClientID)
	if err != nil {
		return g.Error(err, "could not render client_id")
	}

	clientSecret, err := a.renderString(a.ClientSecret)
	if err != nil {
		return g.Error(err, "could not render client_secret")
	}

	redirectURI, err := a.renderString(a.RedirectURI)
	if err != nil {
		return g.Error(err, "could not render redirect_uri")
	}

	authURL, err := a.renderString(a.AuthenticationURL) // Token URL
	if err != nil {
		return g.Error(err, "could not render authentication_url")
	}

	authorizeURL, err := a.renderString(a.AuthorizationURL)
	if err != nil {
		return g.Error(err, "could not render authorization_url")
	}

	deviceAuthURL, err := a.renderString(a.DeviceAuthURL)
	if err != nil {
		return g.Error(err, "could not render device_auth_url")
	}

	// Derive authorization URL if not provided
	if authorizeURL == "" && authURL != "" {
		authorizeURL = strings.Replace(authURL, "/token", "/authorize", 1)
		if authorizeURL == authURL {
			if strings.HasSuffix(authURL, "/oauth/token") {
				authorizeURL = strings.Replace(authURL, "/oauth/token", "/oauth/authorize", 1)
			} else {
				baseURL := strings.TrimSuffix(authURL, "/token")
				authorizeURL = baseURL + "/authorize"
			}
		}
	}

	// Derive device auth URL if not provided
	if deviceAuthURL == "" && authURL != "" {
		deviceAuthURL = strings.Replace(authURL, "/token", "/device/code", 1)
	}

	// Create OAuth2 config
	conf := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       a.Scopes,
		Endpoint: oauth2.Endpoint{
			AuthURL:       authorizeURL,
			TokenURL:      authURL,
			DeviceAuthURL: deviceAuthURL,
		},
		RedirectURL: redirectURI,
	}

	// set headers on return
	defer func() {
		if state.Token != "" {
			state.Headers = map[string]string{"Authorization": g.F("Bearer %s", state.Token)}
		}
	}()

	// load refresh token if found
	storedTok, err := a.LoadToken()
	if err == nil {
		ts := oauth2.ReuseTokenSource(storedTok, conf.TokenSource(ctx, storedTok))
		newTok, err := ts.Token()
		if err == nil {
			state.Token = newTok.AccessToken
			state.Headers = map[string]string{"Authorization": g.F("Bearer %s", newTok.AccessToken)}
			if newTok.RefreshToken != storedTok.RefreshToken {
				a.SaveToken(newTok) // Save if rotated
			}
			g.Debug("Used refreshed token from storage")
			return nil
		}
		g.Warn("Stored refresh token invalid, falling back to authentication")
	}

	if a.Flow == "" {
		a.Flow = OAuthFlowAuthorizationCode
	}

	switch a.Flow {
	case OAuthFlowClientCredentials:
		return a.clientCredentialsFlow(ctx, conf, state)
	case OAuthFlowDeviceCode:
		return a.deviceCodeFlow(ctx, conf, state)
	case OAuthFlowAuthorizationCode:
		return a.authorizationCodeFlow(ctx, conf, state)
	default:
		return g.Error("unsupported OAuth2 flow: %s", a.Flow)
	}
}

// clientCredentialsFlow handles machine-to-machine authentication.
func (a *AuthenticatorOAuth2) clientCredentialsFlow(ctx context.Context, conf *oauth2.Config, state *APIStateAuth) error {
	if conf.ClientSecret == "" {
		return g.Error("client_secret is required for client_credentials flow")
	}
	cc := clientcredentials.Config{
		ClientID:     conf.ClientID,
		ClientSecret: conf.ClientSecret,
		TokenURL:     conf.Endpoint.TokenURL,
		Scopes:       conf.Scopes,
	}

	tok, err := cc.Token(ctx)
	if err != nil {
		return g.Error(err, "failed to get token in client_credentials flow")
	}
	state.Token = tok.AccessToken
	if tok.RefreshToken != "" {
		g.Debug("OAuth refreshToken = %s", tok.RefreshToken)
		a.SaveToken(tok)
	}
	g.Trace("OAuth accessToken = %s", tok.AccessToken)
	return nil
}

// deviceCodeFlow handles authentication for headless environments.
func (a *AuthenticatorOAuth2) deviceCodeFlow(ctx context.Context, conf *oauth2.Config, state *APIStateAuth) error {
	if conf.Endpoint.DeviceAuthURL == "" {
		return g.Error("device_auth_url or derived URL is required for device_code flow")
	}
	dar, err := conf.DeviceAuth(ctx)
	if err != nil {
		return g.Error(err, "failed to get device auth response")
	}
	g.Info("Please visit %s and enter code %s", dar.VerificationURI, dar.UserCode)
	g.Info("If the URL doesn't open, copy and paste it into your browser.")
	tok, err := conf.DeviceAccessToken(ctx, dar)
	if err != nil {
		return g.Error(err, "failed to get token in device_code flow")
	}
	state.Token = tok.AccessToken
	if tok.RefreshToken != "" {
		g.Debug("OAuth refreshToken = %s", tok.RefreshToken)
		a.SaveToken(tok)
	}
	g.Trace("OAuth accessToken = %s", tok.AccessToken)
	return nil
}

// authorizationCodeFlow handles interactive user authentication with browser.
func (a *AuthenticatorOAuth2) authorizationCodeFlow(ctx context.Context, conf *oauth2.Config, state *APIStateAuth) error {
	if conf.Endpoint.AuthURL == "" {
		return g.Error("authorization_url or derived URL is required for authorization_code flow")
	}
	if conf.Endpoint.TokenURL == "" {
		return g.Error("authentication_url is required for authorization_code flow")
	}

	// Use dynamic port for local server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return g.Error(err, "failed to listen on dynamic port")
	}
	port := ln.Addr().(*net.TCPAddr).Port
	conf.RedirectURL = fmt.Sprintf("http://localhost:%d/callback", port)

	// Generate state and PKCE if applicable
	stateVal := generateRandomState()
	var opts []oauth2.AuthCodeOption
	var verifier string
	if conf.ClientSecret == "" { // Enable PKCE for public clients
		verifier = oauth2.GenerateVerifier()
		opts = append(opts, oauth2.S256ChallengeOption(verifier))
	}
	opts = append(opts, oauth2.AccessTypeOffline) // Request refresh token

	authURL := conf.AuthCodeURL(stateVal, opts...)

	// Channels for code and errors
	codeChan := make(chan string)
	errorChan := make(chan error)
	server := http.Server{}

	// Callback handler
	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != stateVal {
			fmt.Fprintf(w, `<h1>Authorization Failed</h1><p>Invalid state parameter.</p>`)
			errorChan <- g.Error("invalid state in callback")
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			fmt.Fprintf(w, `<h1>Authorization Failed</h1><p>No authorization code received.</p>`)
			errorChan <- g.Error("no authorization code received")
			return
		}
		fmt.Fprintf(w, `<h1>Authorization Successful!</h1><p>You can close this window.</p>`)
		codeChan <- code
	})

	// Start server
	go func() {
		server.Handler = http.DefaultServeMux
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			errorChan <- g.Error(err, "failed to start callback server")
		}
	}()

	// Open browser
	g.Info("Opening browser for OAuth2 authorization...")
	g.Info("If the browser doesn't open, visit: %s", authURL)
	if err := a.openBrowser(authURL); err != nil {
		g.Warn("Failed to open browser: %v", err)
		g.Info("Please open: %s", authURL)
	}

	// Wait for code or timeout
	var code string
	select {
	case code = <-codeChan:
		g.Info("Authorization code received, exchanging for token...")
	case err := <-errorChan:
		server.Shutdown(context.Background())
		return err
	case <-time.After(5 * time.Minute):
		server.Shutdown(context.Background())
		return g.Error("authorization timeout after 5 minutes")
	case <-ctx.Done():
		server.Shutdown(context.Background())
		return g.Error("authorization cancelled")
	}

	// Exchange code for token
	exOpts := []oauth2.AuthCodeOption{}
	if verifier != "" {
		exOpts = append(exOpts, oauth2.VerifierOption(verifier))
	}
	tok, err := conf.Exchange(ctx, code, exOpts...)
	if err != nil {
		return g.Error(err, "failed to exchange code for token")
	}
	state.Token = tok.AccessToken
	if tok.RefreshToken != "" {
		g.Debug("OAuth refreshToken = %s", tok.RefreshToken)
		a.SaveToken(tok)
	}
	g.Trace("OAuth accessToken = %s", tok.AccessToken)
	server.Shutdown(context.Background())
	return nil
}

// generateRandomState creates a secure random state string.
func generateRandomState() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

// openBrowser opens the default browser to the given URL (unchanged from original).
func (a *AuthenticatorOAuth2) openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // linux, etc.
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...).Start()
}

// TokenFile returns a secure path for token storage.
func (a *AuthenticatorOAuth2) TokenFile() string {
	dir := filepath.Join(env.HomeDir, "api", "tokens")
	if err := os.MkdirAll(dir, 0755); err != nil {
		g.Warn("could not create folder %s => %s", dir, err.Error())
	}
	return filepath.Join(dir, a.conn.Name+".json")
}

// SaveToken securely saves the token (use keyring for better security).
func (a *AuthenticatorOAuth2) SaveToken(tok *oauth2.Token) error {
	data, err := json.Marshal(tok)
	if err != nil {
		return err
	}
	// For keyring: keyring.Set("sling", a.conn.Name, string(data))
	filePath := a.TokenFile()
	g.Debug("saving token in %s", filePath)
	return os.WriteFile(filePath, data, 0600)
}

// LoadToken loads the stored token.
func (a *AuthenticatorOAuth2) LoadToken() (*oauth2.Token, error) {
	data, err := os.ReadFile(a.TokenFile())
	if err != nil {
		return nil, err
	}
	tok := &oauth2.Token{}
	return tok, json.Unmarshal(data, tok)
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
		props[key], err = a.renderString(val)
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
	cfg, err := iop.MakeAwsConfig(a.conn.Context.Ctx, props)
	if err != nil {
		return g.Error(err, "could not make AWS config for authentication")
	}

	sign := func(iter *Iteration, req *http.Request, bodyBytes []byte) error {
		// Calculate the SHA256 hash of the request body.
		hasher := sha256.New()
		hasher.Write(bodyBytes)
		payloadHash := hex.EncodeToString(hasher.Sum(nil))

		// Create a new signer.
		signer := v4.NewSigner()

		creds, err := cfg.Credentials.Retrieve(iter.context.Ctx)
		if err != nil {
			return g.Error(err, "could not retrieve AWS creds signing request")
		}

		// Sign the request, which adds the 'Authorization' and other necessary headers.
		return signer.SignHTTP(iter.context.Ctx, creds, req, payloadHash, awsService, awsRegion, time.Now())
	}

	if a.endpoint != nil {
		a.endpoint.auth.Sign = sign
	} else {
		a.conn.State.Auth.Sign = sign
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

	var secret string
	if a.Secret != "" {
		secret, err = a.renderString(a.Secret)
		if err != nil {
			return g.Error(err, "could not render secret for HMAC authentication")
		}
	}

	state.Sign = func(iter *Iteration, req *http.Request, bodyBytes []byte) error {
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

			stringToSign, err = iter.renderString(stringToSign)
			if err != nil {
				return g.Error(err, "could not render string for HMAC signer")
			}

			g.Trace(`  rendered HMAC string_to_sign "%s" => %s`, a.SigningString, stringToSign)

			// Compute HMAC-SHA256
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write([]byte(stringToSign))
			signature = hex.EncodeToString(mac.Sum(nil))
		case "sha512":
			stringToSign := g.Rm(a.SigningString, templateMap)

			stringToSign, err = iter.renderString(stringToSign)
			if err != nil {
				return g.Error(err, "could not render string for HMAC signer")
			}

			g.Trace(`  rendered HMAC string_to_sign "%s" => %s`, a.SigningString, stringToSign)

			// Compute HMAC-SHA512
			mac := hmac.New(sha512.New, []byte(secret))
			mac.Write([]byte(stringToSign))
			signature = hex.EncodeToString(mac.Sum(nil))
		default:
			return g.Error("invalid algorithm (%s), only 'sha256' and 'sha512' are supported", a.Algorithm)
		}

		// set signature
		templateMap["signature"] = signature

		g.Trace(`  rendered HMAC template for request headers => %s`, g.Marshal(templateMap))

		// Add headers
		for key, value := range a.ReqHeaders {
			value = g.Rm(value, templateMap)
			value, err = iter.renderString(value)
			if err != nil {
				return g.Error(err, "could not render string for HMAC header: %s", key)
			}
			req.Header.Set(key, value)
			g.Trace(`    rendered HMAC request header "%s" => %s`, key, value)
		}

		return nil
	}

	return
}
