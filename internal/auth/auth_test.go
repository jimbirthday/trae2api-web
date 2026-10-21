package auth

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// 现有 trae-*.json 的真实结构（敏感字段已脱敏为占位）。
const existingFormat = `{
  "account": {"uid": "1000000000000000", "enterpriseId": "test-ent-id", "nickname": "测试用户"},
  "auth": {
    "accessToken": "at-placeholder", "refreshToken": "rt-placeholder", "expiresAt": 1786805537,
    "domain": "trae.cn", "apiHost": "https://api.trae.com.cn",
    "machineId": "abcdef0123456789abcdef0123456789", "deviceId": "0123456789abcdef0123456789abcdef"
  }
}`

func TestParseExistingFormat(t *testing.T) {
	a, err := Parse([]byte(existingFormat))
	if err != nil {
		t.Fatalf("parse existing format: %v", err)
	}
	if a.UID != "1000000000000000" || a.EnterpriseID != "test-ent-id" || a.Nickname != "测试用户" {
		t.Errorf("account: %+v", a)
	}
	if a.AccessToken != "at-placeholder" || a.RefreshToken != "rt-placeholder" || a.ExpiresAt != 1786805537 {
		t.Errorf("tokens: %+v", a)
	}
	if a.Domain != "trae.cn" || a.ApiHost != "https://api.trae.com.cn" {
		t.Errorf("hosts: %+v", a)
	}
	if len(a.MachineID) != 32 || len(a.DeviceID) != 32 {
		t.Errorf("ids: machine=%q device=%q", a.MachineID, a.DeviceID)
	}
}

func TestParseFlat(t *testing.T) {
	raw := []byte(`{"accessToken":"at","refreshToken":"rt","expiresAt":1753600000,"uid":"u2","nickname":"n2","machineId":"m1","deviceId":"d1"}`)
	a, err := Parse(raw)
	if err != nil || a.UID != "u2" || a.AccessToken != "at" || a.MachineID != "m1" || a.DeviceID != "d1" {
		t.Fatalf("flat: %+v %v", a, err)
	}
}

func TestParseMissingToken(t *testing.T) {
	if _, err := Parse([]byte(`{"uid":"u3"}`)); err == nil {
		t.Fatal("want error for missing accessToken")
	}
	if _, err := Parse([]byte(``)); err == nil {
		t.Fatal("want error for empty storage")
	}
}

func TestSaveAtomicRoundtripPreservesSOLOFields(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "trae-u1.json")
	a := &Auth{
		AccessToken: "at", RefreshToken: "rt", ExpiresAt: 1786805537,
		Domain: "trae.cn", ApiHost: "https://api.trae.com.cn",
		MachineID: "m123", DeviceID: "d456",
		UID: "u1", EnterpriseID: "e1", Nickname: "n1", FilePath: fp,
	}
	if err := a.SaveAtomic(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := os.Stat(fp + ".tmp"); !os.IsNotExist(err) {
		t.Error("tmp file should not remain")
	}
	if fi, err := os.Stat(fp); err != nil {
		t.Errorf("file stat err=%v", err)
	} else if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
		t.Errorf("file mode=%v want 0600", fi.Mode().Perm())
	}
	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	b, err := Parse(raw)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if b.AccessToken != "at" || b.UID != "u1" || b.EnterpriseID != "e1" {
		t.Errorf("roundtrip: %+v", b)
	}
	if b.MachineID != "m123" || b.DeviceID != "d456" || b.ApiHost != "https://api.trae.com.cn" {
		t.Errorf("SOLO fields lost: %+v", b)
	}
}

func TestLoadDir(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "trae-u1.json"), []byte(existingFormat), 0o600)
	os.WriteFile(filepath.Join(dir, "trae-u2.json"), []byte(`{"account":{"uid":"u2"},"auth":{"accessToken":"at2","refreshToken":"r"}}`), 0o600)
	os.WriteFile(filepath.Join(dir, "trae-bad.json"), []byte(`not json`), 0o600)
	os.WriteFile(filepath.Join(dir, "other-ignored.json"), []byte(existingFormat), 0o600)

	list, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("want 2 trae accounts, got %+v", list)
	}
	if list[0].FilePath == "" {
		t.Error("FilePath not set")
	}
}

func TestNeedsRefresh(t *testing.T) {
	// 分支 1：过期时刻未知但能刷新 → 需要刷新（按未知过期处理）。
	a := &Auth{ExpiresAt: 0, RefreshToken: "rt"}
	if !a.NeedsRefresh(0) {
		t.Error("unknown expiry with refreshToken should need refresh")
	}

	// 分支 2：过期时刻未知且无法刷新 → 不刷新（硬刷新只会被上游打死）。
	b := &Auth{ExpiresAt: 0}
	if b.NeedsRefresh(0) {
		t.Error("unknown expiry without refreshToken must not need refresh")
	}

	// 分支 3：已知过期时刻 → 按窗口判断。
	a = &Auth{ExpiresAt: 9999999999, RefreshToken: "rt"}
	if a.NeedsRefresh(0) {
		t.Error("far future should not need refresh")
	}
	a.ExpiresAt = 1
	if !a.NeedsRefresh(24 * time.Hour) {
		t.Error("past expiry should need refresh")
	}
	// 临近过期（within 窗口内）也要刷新
	a.ExpiresAt = time.Now().Add(10 * time.Minute).Unix()
	if !a.NeedsRefresh(time.Hour) {
		t.Error("expiring within window should need refresh")
	}
	if a.NeedsRefresh(time.Minute) {
		t.Error("not expiring inside 1m window")
	}
}

// TestValidateUID 覆盖路径穿越与非法字符。
func TestValidateUID(t *testing.T) {
	good := []string{"1", "4122538516165401", "u-1_2", strings.Repeat("a", MaxUIDLen)}
	for _, uid := range good {
		if err := ValidateUID(uid); err != nil {
			t.Errorf("ValidateUID(%q) = %v, want nil", uid, err)
		}
	}
	bad := []string{
		"",                               // 空
		"../../etc/passwd",               // 穿越
		"..",                             // 上级目录
		"a/b",                            // 路径分隔符
		"a\\b",                           // Windows 分隔符
		"a.b",                            // 点号
		"a b",                            // 空格
		"a\x00b",                         // NUL
		"号461",                           // 非 ASCII
		strings.Repeat("a", MaxUIDLen+1), // 超长
	}
	for _, uid := range bad {
		if err := ValidateUID(uid); err == nil {
			t.Errorf("ValidateUID(%q) = nil, want error", uid)
		}
	}
}

// TestFilePathForRejectsTraversal 保证非法 UID 不会拼出越界路径。
func TestFilePathForRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	for _, uid := range []string{"../../etc/passwd", "a/b", "", "..", strings.Repeat("x", MaxUIDLen+1)} {
		if fp := FilePathFor(dir, uid); fp != "" {
			t.Errorf("FilePathFor(%q) = %q, want empty", uid, fp)
		}
		if fp, err := SafeFilePathFor(dir, uid); err == nil {
			t.Errorf("SafeFilePathFor(%q) = %q, want error", uid, fp)
		}
	}
	fp, err := SafeFilePathFor(dir, "4122538516165401")
	if err != nil {
		t.Fatalf("valid uid: %v", err)
	}
	if want := filepath.Join(dir, "trae-4122538516165401.json"); fp != want {
		t.Errorf("got %q want %q", fp, want)
	}
}

// TestExpiresAtStringAndMillis 覆盖字符串/毫秒两种 expiresAt 形态。
func TestExpiresAtStringAndMillis(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int64
	}{
		{"number seconds", `{"accessToken":"at","uid":"u","expiresAt":1793756328}`, 1793756328},
		{"number millis", `{"accessToken":"at","uid":"u","expiresAt":1793756328612}`, 1793756328},
		{"string millis", `{"accessToken":"at","uid":"u","expiresAt":"1793756328612"}`, 1793756328},
		{"string seconds", `{"accessToken":"at","uid":"u","expiresAt":"1793756328"}`, 1793756328},
		{"empty string", `{"accessToken":"at","uid":"u","expiresAt":""}`, 0},
		{"missing", `{"accessToken":"at","uid":"u"}`, 0},
		{"nested string millis", `{"auth":{"accessToken":"at","expiresAt":"1793756328612"},"account":{"uid":"u"}}`, 1793756328},
	}
	for _, c := range cases {
		a, err := Parse([]byte(c.raw))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if a.ExpiresAt != c.want {
			t.Errorf("%s: ExpiresAt=%d want %d", c.name, a.ExpiresAt, c.want)
		}
	}
}

// TestParseDerivesUIDFromJWT 覆盖"没有 uid 字段时从 JWT 的 data.id 推导"。
func TestParseDerivesUIDFromJWT(t *testing.T) {
	jwt := makeTestJWT(t, `{"data":{"id":"287442279345675"},"exp":1793753137}`)
	for _, raw := range []string{
		`{"accessToken":"` + jwt + `"}`,
		`{"auth":{"accessToken":"` + jwt + `"},"account":{}}`,
	} {
		a, err := Parse([]byte(raw))
		if err != nil {
			t.Fatalf("%v", err)
		}
		if a.UID != "287442279345675" {
			t.Errorf("derived uid=%q", a.UID)
		}
	}
	// 推导出的 UID 同样要过校验：JWT 里塞穿越串必须被拒。
	evil := makeTestJWT(t, `{"data":{"id":"../../etc/passwd"}}`)
	if _, err := Parse([]byte(`{"accessToken":"` + evil + `"}`)); err == nil {
		t.Error("uid derived from JWT bypassed ValidateUID")
	}
	// JSON 里显式给的非法 uid 也要被拒。
	if _, err := Parse([]byte(`{"accessToken":"at","uid":"../../etc/passwd"}`)); err == nil {
		t.Error("explicit illegal uid accepted")
	}
}

// TestParseRefreshOnly 覆盖只有 refreshToken 的账号可加载（首次调用再换 accessToken）。
func TestParseRefreshOnly(t *testing.T) {
	a, err := Parse([]byte(`{"auth":{"refreshToken":"rt"},"account":{"uid":"u1"}}`))
	if err != nil {
		t.Fatalf("refresh-only should load: %v", err)
	}
	if a.AccessToken != "" || a.RefreshToken != "rt" || a.UID != "u1" {
		t.Errorf("%+v", a)
	}
	// 两个 token 都没有才算坏文件。
	if _, err := Parse([]byte(`{"account":{"uid":"u1"}}`)); err == nil {
		t.Error("want error when both tokens missing")
	}
}

// TestSetNickname 覆盖持锁更新 + 落盘 + 允许清空。
func TestSetNickname(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "trae-u1.json")
	a := &Auth{AccessToken: "at", RefreshToken: "rt", UID: "u1", Nickname: "旧名", FilePath: fp}
	if err := a.SetNickname("新 名"); err != nil {
		t.Fatalf("SetNickname: %v", err)
	}
	if a.Nickname != "新 名" {
		t.Errorf("nickname=%q", a.Nickname)
	}
	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	b, err := Parse(raw)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if b.Nickname != "新 名" {
		t.Errorf("persisted nickname=%q", b.Nickname)
	}
	// 空串可清空
	if err := a.SetNickname(""); err != nil {
		t.Fatalf("SetNickname(empty): %v", err)
	}
	raw, _ = os.ReadFile(fp)
	if b, _ = Parse(raw); b.Nickname != "" {
		t.Errorf("nickname not cleared: %q", b.Nickname)
	}
	// 未设置 FilePath 时报错而不是静默成功
	c := &Auth{AccessToken: "at", UID: "u2"}
	if err := c.SetNickname("x"); err == nil {
		t.Error("want error when FilePath empty")
	}
}

// TestSaveAtomicDurabilityAndMode 覆盖 fsync 写回后原样可读 + 0600 + 旧文件不被破坏。
func TestSaveAtomicDurabilityAndMode(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "trae-u9.json")
	a := &Auth{AccessToken: "at-old", RefreshToken: "rt-old", UID: "u9", FilePath: fp}
	if err := a.SaveAtomic(); err != nil {
		t.Fatalf("first save: %v", err)
	}
	a.AccessToken = "at-new"
	a.RefreshToken = "rt-new"
	if err := a.SaveAtomic(); err != nil {
		t.Fatalf("second save: %v", err)
	}
	if _, err := os.Stat(fp + ".tmp"); !os.IsNotExist(err) {
		t.Error("tmp file left behind")
	}
	fi, err := os.Stat(fp)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
		t.Errorf("mode=%v want 0600", fi.Mode().Perm())
	}
	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	b, err := Parse(raw)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if b.AccessToken != "at-new" || b.RefreshToken != "rt-new" || b.UID != "u9" {
		t.Errorf("roundtrip: %+v", b)
	}
}

// TestConcurrentRefreshAndReads 在 -race 下验证 token 字段读写并发安全：
// 并发 RefreshToken（写锁）与 JWT/NeedsRefresh/SaveAtomic（读锁/写锁）无数据竞争。
func TestConcurrentRefreshAndReads(t *testing.T) {
	a := &Auth{
		AccessToken:  "at",
		RefreshToken: "rt",
		ExpiresAt:    time.Now().Add(-time.Minute).Unix(),
		FilePath:     filepath.Join(t.TempDir(), "trae-race.json"),
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { // 模拟 RefreshToken 写路径（改写 token 字段）
			defer wg.Done()
			a.Lock()
			a.AccessToken = "new-at"
			a.RefreshToken = "new-rt"
			a.ExpiresAt = time.Now().Add(time.Hour).Unix()
			a.Unlock()
		}()
		wg.Add(1)
		go func() { // 模拟读路径
			defer wg.Done()
			_ = a.JWT()
			_ = a.RefreshTokenValue()
			_ = a.NeedsRefresh(time.Hour)
		}()
	}
	wg.Add(1)
	go func() { // 模拟并发落盘（写锁）
		defer wg.Done()
		_ = a.SaveAtomic()
	}()
	wg.Wait()
	if a.RefreshTokenValue() == "" {
		t.Error("refresh token should not be empty")
	}
}

// TestNeedsRefreshLocked 验证持锁内部版本与外部版本等价。
func TestNeedsRefreshLocked(t *testing.T) {
	a := &Auth{ExpiresAt: time.Now().Add(-time.Minute).Unix()}
	a.mu.RLock()
	need := a.NeedsRefreshLocked(time.Hour)
	a.mu.RUnlock()
	if !need {
		t.Error("expired token should need refresh under lock")
	}
}
