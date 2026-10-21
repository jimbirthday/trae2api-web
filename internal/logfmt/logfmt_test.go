package logfmt

import "testing"

func TestUID8AndLabel(t *testing.T) {
	if got := UID8(""); got != "-" {
		t.Fatalf("UID8(empty)=%q", got)
	}
	if got := UID8("4122538516165401"); got != "41225385" {
		t.Fatalf("UID8=%q", got)
	}
	if got := UID8("abc"); got != "abc" {
		t.Fatalf("UID8(short)=%q", got)
	}
	if got := Label("4122538516165401", "用户5886"); got != "用户5886(41225385)" {
		t.Fatalf("Label=%q", got)
	}
	if got := Label("4122538516165401", "  "); got != "41225385" {
		t.Fatalf("Label(empty nick)=%q", got)
	}
	if got := Label("", ""); got != "-" {
		t.Fatalf("Label(empty)=%q", got)
	}
}

func TestTruncateRuneBoundary(t *testing.T) {
	s := "将在 2026-10-21 重置"
	got := Truncate(s, 5)
	for _, r := range got {
		if r == '\uFFFD' {
			t.Fatalf("truncate broke rune: %q", got)
		}
	}
	if len(got) > 5 {
		t.Fatalf("too long: %q", got)
	}
	if Truncate("  abc  ", 10) != "abc" {
		t.Fatalf("should trim: %q", Truncate("  abc  ", 10))
	}
	if Truncate("abc", 0) != "" {
		t.Fatal("n<=0 should be empty")
	}
}

func TestDisplayWidthAndPad(t *testing.T) {
	if got := DisplayWidth("abc"); got != 3 {
		t.Fatalf("width(abc)=%d", got)
	}
	if got := DisplayWidth("中文"); got != 4 {
		t.Fatalf("width(中文)=%d", got)
	}
	if got := Pad("中文", 8); got != "中文    " {
		t.Fatalf("pad=%q", got)
	}
	if got := Pad("abcdefgh", 4); got != "abcdefgh" {
		t.Fatalf("pad must not truncate: %q", got)
	}
}

func TestShortUA(t *testing.T) {
	cases := map[string]string{
		"":                       "",
		"curl/8.4.0":             "curl/8.4.0",
		"node":                   "node",
		"python-requests/2.31.0": "python-requests/2.31.0",
		"Mozilla/5.0 (Macintosh) Chrome/120.0.0.0 Safari/537.36": "Chrome/120.0.0.0",
	}
	for in, want := range cases {
		if got := ShortUA(in); got != want {
			t.Fatalf("ShortUA(%q)=%q want %q", in, got, want)
		}
	}
}
