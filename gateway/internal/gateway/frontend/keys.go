package frontend

import (
	"encoding/base64"
	"net/http"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakeRepoKeysHandler creates an HTTP handler for serving repository public keys
// This endpoint allows publishers to fetch the .pub and .crt files needed to
// set up a publisher connected to the gateway, without manual file copying.
//
// The endpoint is opt-in: it must be enabled via the "enable_key_endpoint"
// configuration option in user.json.
func MakeRepoKeysHandler(services be.ActionController, enabled bool) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		ctx := h.Context()
		msg := make(map[string]interface{})

		if !enabled {
			msg["status"] = "error"
			msg["reason"] = "key_endpoint_disabled"
			gw.LogC(ctx, "http", gw.LogInfo).Msg("key endpoint is disabled")
			replyJSON(ctx, w, msg)
			return
		}

		repoName := ps.ByName("name")
		if repoName == "" {
			msg["status"] = "error"
			msg["reason"] = "missing_repository_name"
			replyJSON(ctx, w, msg)
			return
		}

		// Check that the repository exists
		rc, err := services.GetRepo(ctx, repoName)
		if err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
			replyJSON(ctx, w, msg)
			return
		}
		if rc == nil {
			msg["status"] = "error"
			msg["reason"] = "invalid_repo"
			replyJSON(ctx, w, msg)
			return
		}

		keysDir := "/etc/cvmfs/keys"

		keyData := make(map[string]string)

		// Read the public key (.pub)
		pubKeyPath := keysDir + "/" + repoName + ".pub"
		if data, err := os.ReadFile(pubKeyPath); err == nil {
			keyData["pub"] = base64.StdEncoding.EncodeToString(data)
		} else {
			gw.LogC(ctx, "http", gw.LogError).
				Err(err).
				Str("path", pubKeyPath).
				Msg("could not read public key file")
			msg["status"] = "error"
			msg["reason"] = "public_key_not_found"
			replyJSON(ctx, w, msg)
			return
		}

		// Read the certificate (.crt)
		crtPath := keysDir + "/" + repoName + ".crt"
		if data, err := os.ReadFile(crtPath); err == nil {
			keyData["crt"] = base64.StdEncoding.EncodeToString(data)
		} else {
			gw.LogC(ctx, "http", gw.LogError).
				Err(err).
				Str("path", crtPath).
				Msg("could not read certificate file")
			msg["status"] = "error"
			msg["reason"] = "certificate_not_found"
			replyJSON(ctx, w, msg)
			return
		}

		msg["status"] = "ok"
		msg["data"] = keyData

		gw.LogC(ctx, "http", gw.LogInfo).
			Str("repo", repoName).
			Msg("served repository keys")

		replyJSON(ctx, w, msg)
	}
}
