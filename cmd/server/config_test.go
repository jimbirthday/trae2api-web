package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	c := Default()
	if c.Listen != ":8000" {
		t.Errorf("listen=%s", c.Listen)
	}
	if c.DefaultModel != "glm-5.2" {
		t.Errorf("default_model=%s", c.DefaultModel)
	}
	if err := c.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if c.PlanCreditDur.Hours() != 12 {
		t.Errorf("plan_credit=%v", c.PlanCreditDur)
	}
	if c.SoftRateDur.Seconds() != 60 {
		t.Errorf("soft_rate=%v", c.SoftRateDur)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"listen":":9999","default_model":"kimi-k2.7-code","schedule":{"checkin_hour":7}}`), 0o600)
	c, err := Load(fp)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":9999" || c.DefaultModel != "kimi-k2.7-code" || c.Schedule.CheckinHour != 7 {
		t.Errorf("c=%+v", c)
	}
	// APIKey 不读 json
	if c.APIKey != "" {
		t.Errorf("api_key should not come from json, got %q", c.APIKey)
	}
}

func TestLoadMissingFileFallsBackToDefaults(t *testing.T) {
	c, err := Load("/nonexistent/config.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8000" {
		t.Errorf("listen=%s", c.Listen)
	}
}

func TestAPIKeyOnlyFromEnv(t *testing.T) {
	t.Setenv("TW2A_API_KEY", "test-key")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.APIKey != "test-key" {
		t.Errorf("api_key=%q", c.APIKey)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("TW2A_LISTEN", ":7777")
	t.Setenv("TW2A_DEFAULT_MODEL", "qwen-3.7-plus")
	t.Setenv("TW2A_SOFT_RATE", "30s")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":7777" || c.DefaultModel != "qwen-3.7-plus" {
		t.Errorf("c=%+v", c)
	}
	if c.SoftRateDur.Seconds() != 30 {
		t.Errorf("soft_rate=%v", c.SoftRateDur)
	}
}

func TestBadDuration(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"cooldown":{"plan_credit":"not-a-duration"}}`), 0o600)
	if _, err := Load(fp); err == nil {
		t.Fatal("want error for bad duration")
	}
}

func TestEnvIntInvalidIsReported(t *testing.T) {
	t.Setenv("TW2A_MAX_IN_FLIGHT", "abc")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	// 非法值不生效，但必须留痕（IgnoredEnv），否则运维无从发现配错。
	if c.Pool.MaxInFlight != 2 {
		t.Errorf("max_in_flight=%d, want default 2", c.Pool.MaxInFlight)
	}
	found := false
	for _, k := range c.IgnoredEnv {
		if k == "TW2A_MAX_IN_FLIGHT" {
			found = true
		}
	}
	if !found {
		t.Errorf("ignored env not recorded: %v", c.IgnoredEnv)
	}
}

func TestEnvNewKeysSupported(t *testing.T) {
	t.Setenv("TW2A_BREAKER_COOLDOWN_MAX", "2h")
	t.Setenv("TW2A_DEGRADE_THRESHOLD", "7")
	t.Setenv("TW2A_DEGRADE_WINDOW", "20m")
	t.Setenv("TW2A_SESSION_GC_INTERVAL", "7m")
	t.Setenv("TW2A_REQLOG_FILE_MAX_MB", "8")
	t.Setenv("TW2A_SCHEDULE_REFRESH_HOURS", "2,14")
	t.Setenv("TW2A_SCHEDULE_BALANCE_HOURS", "9, 21")
	t.Setenv("TW2A_PUBLIC_BASE_URL", "https://trae.example.com/")
	t.Setenv("TW2A_CORS_ORIGINS", " https://a.example.com , https://b.example.com ")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.BreakerMaxDur.Hours() != 2 {
		t.Errorf("breaker_cooldown_max=%v", c.BreakerMaxDur)
	}
	if c.Pool.DegradeThreshold != 7 || c.DegradeWindowDur.Minutes() != 20 {
		t.Errorf("degrade=%d/%v", c.Pool.DegradeThreshold, c.DegradeWindowDur)
	}
	if c.SessionGCDuration.Minutes() != 7 {
		t.Errorf("gc=%v", c.SessionGCDuration)
	}
	if c.ReqLog.FileMaxMB != 8 {
		t.Errorf("file_max_mb=%d", c.ReqLog.FileMaxMB)
	}
	if got := c.Schedule.RefreshHours; len(got) != 2 || got[0] != 2 || got[1] != 14 {
		t.Errorf("refresh_hours=%v", got)
	}
	if got := c.Schedule.BalanceHours; len(got) != 2 || got[0] != 9 || got[1] != 21 {
		t.Errorf("balance_hours=%v", got)
	}
	// 结尾斜杠要去掉，否则拼出 https://host//authorize
	if c.PublicBaseURL != "https://trae.example.com" {
		t.Errorf("public_base_url=%q", c.PublicBaseURL)
	}
	if len(c.CORSOrigins) != 2 || c.CORSOrigins[0] != "https://a.example.com" {
		t.Errorf("cors_origins=%v", c.CORSOrigins)
	}
}

func TestNormalizeClampsAndWarns(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{
		"cooldown":{"soft_rate":"999h"},
		"pool":{"max_in_flight":9999,"breaker_threshold":0,"degrade_threshold":-3},
		"schedule":{"checkin_hour":99,"refresh_hours":[],"balance_hours":[25,-1,8,8]},
		"reqlog":{"file_max_mb":-1,"retain_days":0}
	}`), 0o600)
	c, err := Load(fp)
	if err != nil {
		t.Fatal(err)
	}
	if c.SoftRateDur > 24*time.Hour {
		t.Errorf("soft_rate not clamped: %v", c.SoftRateDur)
	}
	if c.Pool.MaxInFlight != 64 {
		t.Errorf("max_in_flight=%d", c.Pool.MaxInFlight)
	}
	if c.Pool.BreakerThreshold != 3 || c.Pool.DegradeThreshold != 1 {
		t.Errorf("thresholds=%d/%d", c.Pool.BreakerThreshold, c.Pool.DegradeThreshold)
	}
	if c.Schedule.CheckinHour != 9 {
		t.Errorf("checkin_hour=%d", c.Schedule.CheckinHour)
	}
	if got := c.Schedule.RefreshHours; len(got) != 1 || got[0] != 3 {
		t.Errorf("refresh_hours fallback=%v", got)
	}
	if got := c.Schedule.BalanceHours; len(got) != 1 || got[0] != 8 {
		t.Errorf("balance_hours=%v", got)
	}
	if c.ReqLog.FileMaxMB != 16 || c.ReqLog.RetainDays != 3 {
		t.Errorf("reqlog=%d/%d", c.ReqLog.FileMaxMB, c.ReqLog.RetainDays)
	}
	if len(c.Warnings) == 0 {
		t.Error("expected normalization warnings")
	}
}

func TestNormalizeRejectsZeroDuration(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"session":{"ttl":"0s"}}`), 0o600)
	if _, err := Load(fp); err == nil {
		t.Fatal("want error for zero duration")
	}
}

func TestModelErrorRuleRequiresKind(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"upstream":{"model_error_rules":[{"code":3003}]}}`), 0o600)
	if _, err := Load(fp); err == nil {
		t.Fatal("want error for empty rule kind")
	}
}

func TestPublicBaseURLRequiresScheme(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"public_base_url":"trae.example.com"}`), 0o600)
	if _, err := Load(fp); err == nil {
		t.Fatal("want error for scheme-less public_base_url")
	}
}

func TestCallbackPortValidation(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"callback_port":"abc"}`), 0o600)
	if _, err := Load(fp); err == nil {
		t.Fatal("want error for bad callback_port")
	}
	// "0" 是合法值（关闭回调 server）
	os.WriteFile(fp, []byte(`{"callback_port":"0"}`), 0o600)
	if _, err := Load(fp); err != nil {
		t.Fatalf("callback_port=0 should be valid: %v", err)
	}
}

func TestCallbackBindDefault(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.CallbackBind != "0.0.0.0" {
		t.Errorf("callback_bind=%q, want 0.0.0.0", c.CallbackBind)
	}
}

// TestEveryEnvVarDocumented 保证「代码里支持的 env」和「README 配置表」不会漂移：
// 任何新增的 TW2A_* 变量若忘了写进 README，这个测试就红。
func TestEveryEnvVarDocumented(t *testing.T) {
	raw, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`"TW2A_[A-Z0-9_]+"`)
	seen := map[string]bool{}
	for _, m := range re.FindAllString(string(raw), -1) {
		seen[strings.Trim(m, `"`)] = true
	}
	if len(seen) < 20 {
		t.Fatalf("expect many env vars, got %d", len(seen))
	}
	readme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for k := range seen {
		if !strings.Contains(string(readme), k) {
			t.Errorf("env %s 未在 README 配置表中说明", k)
		}
	}
}

// TestExampleConfigIsComplete 保证镜像内置的 config.example.json 覆盖所有 JSON 字段，
// 避免新增配置项后容器里默认配置缺字段（行为与本地跑不一致）。
func TestExampleConfigIsComplete(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("config.example.json 不是合法 JSON: %v", err)
	}
	for _, k := range []string{"listen", "callback_port", "callback_bind", "public_base_url", "cors_origins", "auth_dir", "state_file", "pool", "session", "reqlog", "usage", "schedule", "stream", "upstream"} {
		if _, ok := m[k]; !ok {
			t.Errorf("config.example.json 缺少字段 %q", k)
		}
	}
	// 示例配置必须能通过 normalize（否则镜像默认配置直接启动失败）。
	dir := t.TempDir()
	fp := filepath.Join(dir, "config.example.json")
	if err := os.WriteFile(fp, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(fp); err != nil {
		t.Fatalf("config.example.json 无法通过校验: %v", err)
	}
}

// TestBadErrThresholdDoesNotClobberBreaker 回归：TW2A_ERR_THRESHOLD 解析失败时
// 不能拿默认值覆盖 json 里显式配置的 pool.breaker_threshold。
func TestBadErrThresholdDoesNotClobberBreaker(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "c.json")
	os.WriteFile(fp, []byte(`{"pool":{"breaker_threshold":5}}`), 0o600)
	t.Setenv("TW2A_ERR_THRESHOLD", "oops")
	c, err := Load(fp)
	if err != nil {
		t.Fatal(err)
	}
	if c.Pool.BreakerThreshold != 5 {
		t.Errorf("breaker_threshold=%d, want 5 (json value must survive)", c.Pool.BreakerThreshold)
	}
	if len(c.IgnoredEnv) != 1 || c.IgnoredEnv[0] != "TW2A_ERR_THRESHOLD" {
		t.Errorf("ignored_env=%v", c.IgnoredEnv)
	}
}

func TestInspectAuthDir(t *testing.T) {
	dir := t.TempDir()
	if n, diag := inspectAuthDir(dir); n != 0 || !strings.Contains(diag, "为空") {
		t.Errorf("empty dir: n=%d diag=%q", n, diag)
	}
	os.WriteFile(filepath.Join(dir, "trae-1.json"), []byte("{}"), 0o600)
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600)
	if n, _ := inspectAuthDir(dir); n != 1 {
		t.Errorf("n=%d, want 1 (只数 trae-*.json)", n)
	}
	if n, diag := inspectAuthDir(filepath.Join(dir, "missing")); n != 0 || !strings.Contains(diag, "不存在") {
		t.Errorf("missing dir: n=%d diag=%q", n, diag)
	}
}

func TestCallbackSummary(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"off", Config{CallbackPort: "0"}, "off(手动粘贴)"},
		{"public", Config{CallbackPort: "18080", PublicBaseURL: "https://t.example.com"}, "https://t.example.com/authorize"},
		{"bind", Config{CallbackPort: "18080", CallbackBind: "0.0.0.0"}, "http://0.0.0.0:18080/authorize"},
	}
	for _, tc := range cases {
		if got := callbackSummary(&tc.cfg); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}
