package auth

import (
	"testing"
)

// 中转面板导出格式（trae-cn-relay-accounts v1）整包解析。
func TestParseRelayBatch(t *testing.T) {
	raw := []byte(`{
	  "format": "trae-cn-relay-accounts",
	  "version": 1,
	  "accounts": [
	    {"user_id":"4122538516165401","label":"用户58861327585","token":"jwt-aaa",
	     "refresh_token":"rt-aaa","expired_at":"1793756328612","host":"https://api.trae.com.cn","edition":"cn"},
	    {"user_id":"999","label":"","token":"jwt-bbb","refresh_token":"","expired_at":"","host":""}
	  ]
	}`)
	if !LooksLikeRelay(raw) {
		t.Fatal("LooksLikeRelay=false")
	}
	list, err := ParseRelay(raw)
	if err != nil {
		t.Fatalf("ParseRelay: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("len=%d", len(list))
	}
	a := list[0]
	if a.UID != "4122538516165401" || a.AccessToken != "jwt-aaa" || a.RefreshToken != "rt-aaa" {
		t.Fatalf("unexpected: %+v", a)
	}
	if a.Nickname != "用户58861327585" {
		t.Fatalf("nickname=%q", a.Nickname)
	}
	// 毫秒字符串 → 秒
	if a.ExpiresAt != 1793756328 {
		t.Fatalf("expiresAt=%d", a.ExpiresAt)
	}
	if a.Domain != "trae.cn" || a.ApiHost != "https://api.trae.com.cn" {
		t.Fatalf("host/domain: %q %q", a.ApiHost, a.Domain)
	}
	// 第二条：无 label → 用 uid 末 4 位补昵称；空 expired_at → 0
	if list[1].Nickname != "用户999" && list[1].Nickname != "用户 999" && list[1].ExpiresAt != 0 {
		t.Fatalf("second item: %+v", list[1])
	}
	if list[1].ExpiresAt != 0 {
		t.Fatalf("expiresAt=%d", list[1].ExpiresAt)
	}
}

// 单条中转账号（无 accounts 包裹）也要能解析。
func TestParseRelaySingle(t *testing.T) {
	raw := []byte(`{"user_id":"1","token":"jwt","refresh_token":"rt","expired_at":1793756328612}`)
	list, err := ParseRelay(raw)
	if err != nil || len(list) != 1 {
		t.Fatalf("err=%v len=%d", err, len(list))
	}
	if list[0].ExpiresAt != 1793756328 {
		t.Fatalf("expiresAt=%d", list[0].ExpiresAt)
	}
}

// 本仓库嵌套形绝不能被判为 relay 格式。
func TestLooksLikeRelayRejectsNested(t *testing.T) {
	raw := []byte(`{"auth":{"accessToken":"x"},"account":{"uid":"1"}}`)
	if LooksLikeRelay(raw) {
		t.Fatal("nested form misdetected as relay")
	}
}

// 缺 token → 报错而不是产出空账号。
func TestParseRelayMissingToken(t *testing.T) {
	raw := []byte(`{"accounts":[{"user_id":"1"}]}`)
	if _, err := ParseRelay(raw); err == nil {
		t.Fatal("want error")
	}
}

// 导出包（{"format":"trae2api-web-accounts","accounts":[{auth,account}]}）
// 必须能原样反向导入：导出→导入闭环。
func TestParseRelayAcceptsExportedPackage(t *testing.T) {
	raw := []byte(`{"format":"trae2api-web-accounts","version":1,"accounts":[
	  {"auth":{"accessToken":"at-x","refreshToken":"rt-x","expiresAt":1793756328,
	           "domain":"trae.cn","apiHost":"https://api.trae.com.cn"},
	   "account":{"uid":"42","nickname":"甲","weight":5,"note":"n"}}
	]}`)
	if !LooksLikeRelay(raw) {
		t.Fatal("exported package not detected")
	}
	list, err := ParseRelay(raw)
	if err != nil {
		t.Fatalf("ParseRelay: %v", err)
	}
	if len(list) != 1 || list[0].UID != "42" || list[0].AccessToken != "at-x" {
		t.Fatalf("%+v", list)
	}
	if list[0].Weight != 5 || list[0].Note != "n" || list[0].Nickname != "甲" {
		t.Fatalf("profile lost: %+v", list[0])
	}
}

// 格式 A：{"accounts":[{"name":..,"token":..,"refreshToken":..,"cookies":[..]}]}
// 没有 user_id，uid 从 JWT 的 data.id 解析；有效期回退到 JWT 的 exp。
func TestParseRelayFormatA(t *testing.T) {
	jwt := makeTestJWT(t, `{"data":{"id":"287442279345675","tenant_id":"7o2d894p7dr0o4","type":"user"},"exp":1793753137,"iat":1792543537}`)
	raw := []byte(`{"accounts":[{"name":"账号461","token":"` + jwt + `","refreshToken":"wb5-rt",
	  "deviceId":"","machineId":"","enabled":true,
	  "cookies":[{"name":"sessionid","value":"abc","host_key":".trae.cn","path":"/","is_secure":false,"is_httponly":true}]}]}`)
	if !LooksLikeRelay(raw) {
		t.Fatal("格式 A 未被识别")
	}
	list, err := ParseAnyAccounts(raw)
	if err != nil || len(list) != 1 {
		t.Fatalf("err=%v len=%d", err, len(list))
	}
	a := list[0]
	if a.UID != "287442279345675" {
		t.Fatalf("uid=%q", a.UID)
	}
	if a.Nickname != "账号461" || a.RefreshToken != "wb5-rt" {
		t.Fatalf("nick/refresh=%q/%q", a.Nickname, a.RefreshToken)
	}
	if a.ExpiresAt != 1793753137 {
		t.Fatalf("expiresAt=%d", a.ExpiresAt)
	}
	if len(a.Cookies) != 1 || a.CookieValue("sessionid") != "abc" {
		t.Fatalf("cookies=%+v", a.Cookies)
	}
	if a.ImportDisabled {
		t.Fatal("enabled=true 不该标记停用")
	}
	if a.Domain != "trae.cn" || a.ApiHost != "https://api.trae.com.cn" {
		t.Fatalf("%q %q", a.Domain, a.ApiHost)
	}
}

// enabled=false → ImportDisabled。
func TestParseRelayFormatADisabled(t *testing.T) {
	jwt := makeTestJWT(t, `{"data":{"id":"42"},"exp":1793753137}`)
	raw := []byte(`{"accounts":[{"name":"n","token":"` + jwt + `","refreshToken":"r","enabled":false}]}`)
	list, err := ParseAnyAccounts(raw)
	if err != nil || len(list) != 1 {
		t.Fatalf("err=%v len=%d", err, len(list))
	}
	if !list[0].ImportDisabled {
		t.Fatal("enabled=false 未标记停用")
	}
}

// 多个 JSON 文档首尾相接（用户手工拼接的导出文件）→ 全部解析。
func TestParseAnyAccountsConcatenated(t *testing.T) {
	j1 := makeTestJWT(t, `{"data":{"id":"111"},"exp":1793753137}`)
	j2 := makeTestJWT(t, `{"data":{"id":"222"},"exp":1793753137}`)
	raw := []byte(`{"accounts":[{"name":"a","token":"` + j1 + `"}]}
{"format":"trae-cn-relay-accounts","version":1,"accounts":[{"user_id":"333","token":"` + j2 + `"}]}`)
	list, err := ParseAnyAccounts(raw)
	if err != nil {
		t.Fatalf("ParseAnyAccounts: %v", err)
	}
	got := map[string]bool{}
	for _, a := range list {
		got[a.UID] = true
	}
	if len(list) != 2 || !got["111"] || !got["333"] {
		t.Fatalf("list=%+v", got)
	}
}

// 顶层数组形态。
func TestParseAnyAccountsArray(t *testing.T) {
	j1 := makeTestJWT(t, `{"data":{"id":"555"},"exp":1793753137}`)
	raw := []byte(`[{"name":"a","token":"` + j1 + `","refreshToken":"r1"},{"user_id":"666","token":"` + j1 + `"}]`)
	list, err := ParseAnyAccounts(raw)
	if err != nil {
		t.Fatalf("ParseAnyAccounts: %v", err)
	}
	if len(list) != 2 || list[0].UID != "555" || list[1].UID != "666" {
		t.Fatalf("list=%+v", list)
	}
}

// 坏文档不阻断其余：第一条缺 token 被跳过，第二条成功。
func TestParseAnyAccountsSkipsBadItem(t *testing.T) {
	jwt := makeTestJWT(t, `{"data":{"id":"777"},"exp":1793753137}`)
	raw := []byte(`{"accounts":[{"name":"bad"},{"name":"good","token":"` + jwt + `"}]}`)
	list, err := ParseAnyAccounts(raw)
	if err != nil || len(list) != 1 || list[0].UID != "777" {
		t.Fatalf("err=%v list=%+v", err, list)
	}
}

func TestParseJWTInvalid(t *testing.T) {
	if info := ParseJWT("not-a-jwt"); info.UID != "" || info.Exp != 0 {
		t.Fatalf("%+v", info)
	}
}

// 本仓库自己导出的「扁平单账号」JSON 不能被当成批量中继格式。
//
// 历史缺陷：LooksLikeRelay 曾把 refreshToken（驼峰）当判据，而扁平导出形
// {"uid":..,"accessToken":..,"refreshToken":..} 正好带这个键，于是单账号导入
// 被塞进批量通道、走中继解析器，uid 丢失 → "cannot determine uid"。
func TestLooksLikeRelayRejectsOwnFlatExport(t *testing.T) {
	flat := []byte(`{"uid":"4122538516165401","accessToken":"at","refreshToken":"rt","nickname":"n"}`)
	if LooksLikeRelay(flat) {
		t.Fatal("本仓库扁平导出形被误判为批量中继格式")
	}
	if HasAccounts(flat) {
		t.Fatal("HasAccounts 不应把扁平单账号当成批量")
	}
	a, err := Parse(flat)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a.UID != "4122538516165401" {
		t.Fatalf("uid=%q", a.UID)
	}

	// 反过来：真正的中继/批量格式必须仍然被识别（否则批量导入会退化）。
	for _, s := range []string{
		`{"accounts":[{"token":"t"}]}`,
		`{"user_id":"1","label":"l","token":"t","refresh_token":"rt"}`,
		`{"name":"账号461","token":"t","refreshToken":"rt","cookies":[]}`,
	} {
		if !LooksLikeRelay([]byte(s)) {
			t.Fatalf("应识别为中继格式: %s", s)
		}
		if !HasAccounts([]byte(s)) {
			t.Fatalf("HasAccounts 应为 true: %s", s)
		}
	}
}

// accounts 数组里混放本仓库扁平形时，uid 必须被读出来（而不是只认 user_id/id）。
func TestParseRelayAcceptsFlatUIDInArray(t *testing.T) {
	raw := []byte(`{"accounts":[
	  {"uid":"111","accessToken":"at1","refreshToken":"rt1","nickname":"a"},
	  {"user_id":"222","label":"b","token":"a.b.c","refresh_token":"rt2"}
	]}`)
	list, err := ParseAnyAccounts(raw)
	if err != nil {
		t.Fatalf("ParseAnyAccounts: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("n=%d", len(list))
	}
	if list[0].UID != "111" || list[1].UID != "222" {
		t.Fatalf("uids=%q,%q", list[0].UID, list[1].UID)
	}
	if list[0].AccessToken != "at1" || list[1].AccessToken != "a.b.c" {
		t.Fatalf("tokens=%q,%q", list[0].AccessToken, list[1].AccessToken)
	}
}
