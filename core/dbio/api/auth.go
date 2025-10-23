package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
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
	auth := ac.Spec.Authentication

	// set auth data
	setAuthenticated := func() {
		ac.State.Auth.Authenticated = true

		// Setup auth expiry timer
		if ac.Spec.Authentication.Expires > 0 {
			// Calculate expiry time
			expiryDuration := time.Duration(ac.Spec.Authentication.Expires) * time.Second
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
	}

	switch auth.Type {
	case AuthTypeNone:
		ac.State.Auth.Authenticated = true
		return nil

	case AuthTypeBasic:
		userPass, err := ac.renderString(g.F("%s:%s", auth.Username, auth.Password))
		if err != nil {
			return g.Error(err, "could not render user-password")
		}
		credentialsB64 := base64.StdEncoding.EncodeToString([]byte(userPass))
		ac.State.Auth.Headers = map[string]string{"Authorization": g.F("Basic %s", credentialsB64)}
		setAuthenticated()
		return nil

	case AuthTypeSequence:
		// create ephemeral endpoint
		baseEndpoint := &Endpoint{
			Name:    "api.auth",
			State:   g.M(),
			context: g.NewContext(ac.Context.Ctx),
			conn:    ac,
		}

		g.Debug("running authentication sequence")
		err = runSequence(auth.Sequence, baseEndpoint)
		if err != nil {
			return g.Error(err, "could not auth via sequence")
		}

		// sync state back to APIConnection
		ac.Context.Lock()

		// sync the state section back to ac.State.State
		if stateMap := baseEndpoint.State; len(stateMap) > 0 {
			for k, v := range stateMap {
				ac.State.State[k] = v
			}
		}

		ac.Context.Unlock()
		setAuthenticated()
		return nil

	case AuthTypeOAuth2:
		token, err := ac.performOAuth2Flow(auth)
		if err != nil {
			return g.Error(err, "OAuth2 authentication failed")
		}
		ac.State.Auth.Headers = map[string]string{"Authorization": g.F("Bearer %s", token)}
		ac.State.Auth.Token = token
		setAuthenticated()
		return nil

	case AuthTypeAWSSigV4:

		props := map[string]string{
			"aws_service":           ac.Spec.Authentication.AwsService,
			"aws_access_key_id":     ac.Spec.Authentication.AwsAccessKeyID,
			"aws_secret_access_key": ac.Spec.Authentication.AwsSecretAccessKey,
			"aws_session_token":     ac.Spec.Authentication.AwsSessionToken,
			"aws_region":            ac.Spec.Authentication.AwsRegion,
			"aws_profile":           ac.Spec.Authentication.AwsProfile,
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

		setAuthenticated()

	default:
		setAuthenticated()
		return nil
	}

	return
}

// performOAuth2Flow handles different OAuth2 flows
func (ac *APIConnection) performOAuth2Flow(auth Authentication) (token string, err error) {
	g.Debug("running OAuth2 flow (type=%#v)", auth.Flow)
	switch auth.Flow {
	case OAuthFlowClientCredentials:
		return ac.performClientCredentialsFlow(auth)
	case OAuthFlowRefreshToken:
		return ac.performRefreshTokenFlow(auth)
	case OAuthFlowPassword:
		return ac.performPasswordFlow(auth)
	case OAuthFlowAuthorizationCode:
		return ac.performAuthorizationCodeFlow(auth)
	default:
		return "", g.Error("unsupported OAuth2 flow: %s", auth.Flow)
	}
}

// performClientCredentialsFlow implements the OAuth2 Client Credentials flow
func (ac *APIConnection) performClientCredentialsFlow(auth Authentication) (token string, err error) {
	// Render template values
	clientID, err := ac.renderString(auth.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := ac.renderString(auth.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	authURL, err := ac.renderString(auth.AuthenticationURL)
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
	if len(auth.Scopes) > 0 {
		payload["scope"] = strings.Join(auth.Scopes, " ")
	}

	// Make request
	resp, err := ac.makeOAuthRequest(authURL, payload)
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
func (ac *APIConnection) performRefreshTokenFlow(auth Authentication) (token string, err error) {
	// Render template values
	clientID, err := ac.renderString(auth.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := ac.renderString(auth.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	refreshToken, err := ac.renderString(auth.RefreshToken)
	if err != nil {
		return "", g.Error(err, "could not render refresh_token")
	}

	authURL, err := ac.renderString(auth.AuthenticationURL)
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
	resp, err := ac.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 refresh request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		// Update refresh token if a new one was provided
		if newRefreshToken, ok := resp["refresh_token"].(string); ok {
			ac.State.Auth.Token = newRefreshToken
		}
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 refresh response")
}

// performPasswordFlow implements the OAuth2 Resource Owner Password Credentials flow
func (ac *APIConnection) performPasswordFlow(auth Authentication) (token string, err error) {
	// Render template values
	clientID, err := ac.renderString(auth.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := ac.renderString(auth.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	username, err := ac.renderString(auth.Username)
	if err != nil {
		return "", g.Error(err, "could not render username")
	}

	password, err := ac.renderString(auth.Password)
	if err != nil {
		return "", g.Error(err, "could not render password")
	}

	authURL, err := ac.renderString(auth.AuthenticationURL)
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
	if len(auth.Scopes) > 0 {
		payload["scope"] = strings.Join(auth.Scopes, " ")
	}

	// Make request
	resp, err := ac.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 password request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		// Store refresh token if provided
		if refreshToken, ok := resp["refresh_token"].(string); ok {
			ac.State.Auth.Token = refreshToken
		}
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 password response")
}

// performAuthorizationCodeFlow implements the OAuth2 Authorization Code flow
func (ac *APIConnection) performAuthorizationCodeFlow(auth Authentication) (token string, err error) {
	// we already have refresh token
	if auth.RefreshToken != "" {
		return ac.performRefreshTokenFlow(auth)
	}

	// Check if we should use the interactive server flow
	if auth.RedirectURI == "" || strings.Contains(auth.RedirectURI, "localhost") || strings.Contains(auth.RedirectURI, "127.0.0.1") {
		return ac.performAuthorizationCodeFlowWithServer(auth)
	}

	// This flow assumes the authorization code is already provided in the Token field
	authCode, err := ac.renderString(auth.Token)
	if err != nil {
		return "", g.Error(err, "could not render authorization code")
	}

	if authCode == "" {
		return "", g.Error("authorization code is required for authorization_code flow (provide in token field)")
	}

	return ac.exchangeCodeForToken(auth, authCode)
}

// performAuthorizationCodeFlowWithServer implements the OAuth2 Authorization Code flow with a local server
func (ac *APIConnection) performAuthorizationCodeFlowWithServer(auth Authentication) (token string, err error) {
	// Render template values
	clientID, err := ac.renderString(auth.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := ac.renderString(auth.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	authURL, err := ac.renderString(auth.AuthenticationURL)
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
	auth.RedirectURI = fmt.Sprintf("http://localhost:%d/callback", port)

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
	params.Set("redirect_uri", auth.RedirectURI)
	if len(auth.Scopes) > 0 {
		params.Set("scope", strings.Join(auth.Scopes, " "))
	}
	// Add a state parameter for security (optional but recommended)
	params.Set("state", fmt.Sprintf("sling-%d", time.Now().Unix()))

	// Determine the authorization endpoint (different from token endpoint)
	// First check if authorization_url is explicitly provided
	authorizeURL, err := ac.renderString(auth.AuthorizationURL)
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

	if err := openBrowser(fullAuthURL); err != nil {
		g.Warn("Failed to open browser automatically: %v", err)
		g.Info("Please manually open the following URL in your browser:")
		g.Info("%s", fullAuthURL)
	}

	// Wait for authorization code or timeout
	select {
	case code := <-codeChan:
		g.Info("Authorization code received, exchanging for access token...")
		return ac.exchangeCodeForToken(auth, code)
	case err := <-errorChan:
		return "", err
	case <-time.After(5 * time.Minute):
		server.Shutdown(context.Background())
		return "", g.Error("authorization timeout after 5 minutes")
	case <-ac.Context.Ctx.Done():
		server.Shutdown(context.Background())
		return "", g.Error("authorization cancelled")
	}
}

// exchangeCodeForToken exchanges an authorization code for an access token
func (ac *APIConnection) exchangeCodeForToken(auth Authentication, code string) (token string, err error) {
	// Render template values
	clientID, err := ac.renderString(auth.ClientID)
	if err != nil {
		return "", g.Error(err, "could not render client_id")
	}

	clientSecret, err := ac.renderString(auth.ClientSecret)
	if err != nil {
		return "", g.Error(err, "could not render client_secret")
	}

	redirectURI, err := ac.renderString(auth.RedirectURI)
	if err != nil {
		return "", g.Error(err, "could not render redirect_uri")
	}

	authURL, err := ac.renderString(auth.AuthenticationURL)
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
	resp, err := ac.makeOAuthRequest(authURL, payload)
	if err != nil {
		return "", g.Error(err, "failed to make OAuth2 authorization code request")
	}

	// Extract token from response
	if accessToken, ok := resp["access_token"].(string); ok {
		// Store refresh token if provided
		if refreshToken, ok := resp["refresh_token"].(string); ok {
			// TODO: need to be stored in home folder
			g.Debug("OAuth refreshToken = %s", refreshToken)
			ac.State.Auth.Token = refreshToken
		}
		g.Trace("OAuth accessToken = %s", accessToken)
		return accessToken, nil
	}

	return "", g.Error("no access_token found in OAuth2 authorization code response")
}

// openBrowser opens the default browser to the given URL
func openBrowser(url string) error {
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

// makeOAuthRequest makes an HTTP request for OAuth2 token exchange
func (ac *APIConnection) makeOAuthRequest(url string, payload map[string]string) (response map[string]any, err error) {
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
	ac.Context.Debug("oauth POST request to %s", url)
	ac.Context.Trace("oauth POST request payload: %s", g.Marshal(payload))
	ac.Context.Trace("oauth POST request form data: %s", formData.String())
	req, err := http.NewRequestWithContext(ac.Context.Ctx, "POST", url, strings.NewReader(formData.String()))
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

	ac.Context.Debug("oauth POST response status code: %d", resp.StatusCode)

	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, g.Error(err, "failed to read OAuth2 response body")
	}

	ac.Context.Trace("oauth POST response body: %s", string(body))

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
