package auth

import (
	"encoding/base64"
	"testing"
)

// makeTestJWT 造一个只含 payload 的假 JWT（签名段无关紧要，解析不验签）。
func makeTestJWT(t *testing.T, payload string) string {
	t.Helper()
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	pl := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return hdr + "." + pl + ".sig"
}
