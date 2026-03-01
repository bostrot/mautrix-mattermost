// Package connector implements the bridgev2 network interface for Mattermost.
package connector

import (
	"context"
	"fmt"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// MattermostConnector is the top-level bridgev2.NetworkConnector for Mattermost.
type MattermostConnector struct {
	Bridge *bridgev2.Bridge
	Config *Config
}

var _ bridgev2.NetworkConnector = (*MattermostConnector)(nil)

func (m *MattermostConnector) Init(bridge *bridgev2.Bridge) {
	m.Bridge = bridge
}

func (m *MattermostConnector) Start(ctx context.Context) error {
	return nil
}

func (m *MattermostConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:      "Mattermost",
		NetworkURL:       "https://mattermost.com",
		NetworkID:        "mattermost",
		BeeperBridgeType: "github.com/bostrot/mautrix-mattermost",
		DefaultPort:      29345,
	}
}

func (m *MattermostConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		UserLogin: func() any {
			return &UserLoginMetadata{}
		},
	}
}

func (m *MattermostConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (m *MattermostConnector) GetConfig() (string, any, configupgrade.Upgrader) {
	if m.Config == nil {
		m.Config = &Config{}
	}
	return ExampleConfig, m.Config, configupgrade.SimpleUpgrader(func(configupgrade.Helper) {})
}

func (m *MattermostConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta := login.Metadata.(*UserLoginMetadata)
	login.Client = &MattermostClient{
		UserLogin: login,
		Username:  meta.Username,
		Token:     meta.Token,
		ServerURL: meta.ServerURL,
		UserID:    meta.UserID,
	}
	return nil
}

func (m *MattermostConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{
			Name:        "Login with Personal Access Token",
			Description: "Login using a Mattermost Personal Access Token (PAT) and server URL",
			ID:          "mm-login-pat",
		},
		{
			Name:        "Login with Email & Password",
			Description: "Login using your Mattermost email and password",
			ID:          "mm-login-password",
		},
		{
			Name:        "Login with Browser Cookie (SSO)",
			Description: "Login using the MMAUTHTOKEN cookie from your browser session (for SSO/GitLab login)",
			ID:          "mm-login-cookie",
		},
	}
}

func (m *MattermostConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	switch flowID {
	case "mm-login-pat":
		return &PATLoginProcess{User: user}, nil
	case "mm-login-password":
		return &PasswordLoginProcess{User: user}, nil
	case "mm-login-cookie":
		return &CookieLoginProcess{User: user}, nil
	default:
		return nil, fmt.Errorf("unknown login flow ID: %s", flowID)
	}
}

func (m *MattermostConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

// UserLoginMetadata stores persistent login credentials per user.
type UserLoginMetadata struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Token     string `json:"token"`
	ServerURL string `json:"server_url"`
}

// --- PAT Login ---

// PATLoginProcess handles Personal Access Token-based login.
type PATLoginProcess struct {
	User *bridgev2.User
}

var _ bridgev2.LoginProcessUserInput = (*PATLoginProcess)(nil)

func (p *PATLoginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "com.bostrot.mattermost.enter_pat",
		Instructions: "Enter your Mattermost server URL and Personal Access Token",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type: bridgev2.LoginInputFieldTypeUsername,
					ID:   "server_url",
					Name: "Server URL (e.g. https://mattermost.example.com)",
				},
				{
					Type: bridgev2.LoginInputFieldTypePassword,
					ID:   "token",
					Name: "Personal Access Token",
				},
			},
		},
	}, nil
}

func (p *PATLoginProcess) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	serverURL := input["server_url"]
	token := input["token"]
	if serverURL == "" || token == "" {
		return nil, fmt.Errorf("server_url and token are required")
	}

	// Verify the token by fetching /users/me.
	userID, username, err := mattermost.GetSelf(serverURL, token)
	if err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}

	ul, err := p.User.NewLogin(ctx, &database.UserLogin{
		ID:         networkid.UserLoginID(username),
		RemoteName: username,
		Metadata: &UserLoginMetadata{
			UserID:    userID,
			Username:  username,
			Token:     token,
			ServerURL: serverURL,
		},
	}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(ctx context.Context, login *bridgev2.UserLogin) error {
			login.Client = &MattermostClient{
				UserLogin: login,
				Username:  username,
				Token:     token,
				ServerURL: serverURL,
				UserID:    userID,
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create login: %w", err)
	}
	ul.Client.Connect(ul.Log.WithContext(ctx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "com.bostrot.mattermost.complete",
		Instructions: fmt.Sprintf("Successfully logged in as %s", username),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

func (p *PATLoginProcess) Cancel() {}

// --- Password Login ---

// PasswordLoginProcess handles email + password login.
type PasswordLoginProcess struct {
	User *bridgev2.User
}

var _ bridgev2.LoginProcessUserInput = (*PasswordLoginProcess)(nil)

func (p *PasswordLoginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "com.bostrot.mattermost.enter_credentials",
		Instructions: "Enter your Mattermost server URL, email and password",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type: bridgev2.LoginInputFieldTypeUsername,
					ID:   "server_url",
					Name: "Server URL (e.g. https://mattermost.example.com)",
				},
				{
					Type: bridgev2.LoginInputFieldTypeEmail,
					ID:   "email",
					Name: "Email",
				},
				{
					Type: bridgev2.LoginInputFieldTypePassword,
					ID:   "password",
					Name: "Password",
				},
			},
		},
	}, nil
}

func (p *PasswordLoginProcess) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	serverURL := input["server_url"]
	email := input["email"]
	password := input["password"]
	if serverURL == "" || email == "" || password == "" {
		return nil, fmt.Errorf("server_url, email and password are required")
	}

	token, err := mattermost.LoginWithPassword(serverURL, email, password)
	if err != nil {
		return nil, fmt.Errorf("login failed: %w", err)
	}

	userID, username, err := mattermost.GetSelf(serverURL, token)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user info: %w", err)
	}

	ul, err := p.User.NewLogin(ctx, &database.UserLogin{
		ID:         networkid.UserLoginID(username),
		RemoteName: username,
		Metadata: &UserLoginMetadata{
			UserID:    userID,
			Username:  username,
			Token:     token,
			ServerURL: serverURL,
		},
	}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(ctx context.Context, login *bridgev2.UserLogin) error {
			login.Client = &MattermostClient{
				UserLogin: login,
				Username:  username,
				Token:     token,
				ServerURL: serverURL,
				UserID:    userID,
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create login: %w", err)
	}
	ul.Client.Connect(ul.Log.WithContext(ctx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "com.bostrot.mattermost.complete",
		Instructions: fmt.Sprintf("Successfully logged in as %s", username),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

func (p *PasswordLoginProcess) Cancel() {}

// --- Cookie / SSO Login ---

// CookieLoginProcess handles SSO login via the MMAUTHTOKEN browser cookie.
// The MMAUTHTOKEN value is identical to a Bearer token and can be used directly
// with the Mattermost REST API. MMCSRF is only required for browser cookie-based
// requests and is not needed here.
type CookieLoginProcess struct {
	User *bridgev2.User
}

var _ bridgev2.LoginProcessUserInput = (*CookieLoginProcess)(nil)

func (p *CookieLoginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeUserInput,
		StepID: "com.bostrot.mattermost.enter_cookie",
		Instructions: "Log in to Mattermost in your browser (via SSO/GitLab), then open DevTools " +
			"(F12) → Application → Cookies → select your server → copy the value of MMAUTHTOKEN.",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type: bridgev2.LoginInputFieldTypeUsername,
					ID:   "server_url",
					Name: "Server URL (e.g. https://mattermost.example.com)",
				},
				{
					Type: bridgev2.LoginInputFieldTypePassword,
					ID:   "token",
					Name: "MMAUTHTOKEN cookie value",
				},
			},
		},
	}, nil
}

func (p *CookieLoginProcess) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	serverURL := input["server_url"]
	token := input["token"]
	if serverURL == "" || token == "" {
		return nil, fmt.Errorf("server_url and token are required")
	}

	// MMAUTHTOKEN is accepted as a Bearer token directly.
	userID, username, err := mattermost.GetSelf(serverURL, token)
	if err != nil {
		return nil, fmt.Errorf("failed to verify cookie token: %w", err)
	}

	ul, err := p.User.NewLogin(ctx, &database.UserLogin{
		ID:         networkid.UserLoginID(username),
		RemoteName: username,
		Metadata: &UserLoginMetadata{
			UserID:    userID,
			Username:  username,
			Token:     token,
			ServerURL: serverURL,
		},
	}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(ctx context.Context, login *bridgev2.UserLogin) error {
			login.Client = &MattermostClient{
				UserLogin: login,
				Username:  username,
				Token:     token,
				ServerURL: serverURL,
				UserID:    userID,
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create login: %w", err)
	}
	ul.Client.Connect(ul.Log.WithContext(ctx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "com.bostrot.mattermost.complete",
		Instructions: fmt.Sprintf("Successfully logged in as %s", username),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

func (p *CookieLoginProcess) Cancel() {}

// Config holds any future bridge-specific configuration.
type Config struct{}

const ExampleConfig = `# Mattermost Bridge Configuration
# Additional per-network settings can be added here.
`
