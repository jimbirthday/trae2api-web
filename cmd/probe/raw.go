package main

// raw 探针：绕过 PrepareBody 直接发 llm_utils_chat，用于验证
// 「不同 function / 请求体形态」对上游思考量与首字延迟的影响。

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/upstream"
)

func runRaw(args []string) {
	fs := flag.NewFlagSet("raw", flag.ExitOnError)
	dir := fs.String("auths", "./auths", "auth dir")
	model := fs.String("model", "glm-5.2", "model")
	prompt := fs.String("p", "写一段150字介绍杭州", "prompt")
	fn := fs.String("function", "solo_work_lite", "function name")
	fs.Parse(args)

	auths, err := auth.LoadDir(*dir)
	if err != nil || len(auths) == 0 {
		log.Fatalf("load: %v", err)
	}
	a := auths[0]
	body, _ := json.Marshal(map[string]any{
		"messages":    []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": *prompt}}}},
		"stream":      true,
		"function":    *fn,
		"config_name": *model,
		"model":       *model,
	})
	req, err := http.NewRequest(http.MethodPost, upstream.AgentHost+upstream.EpChat, bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	upstream.SOLOHeaders(req, a, true)
	t0 := time.Now()
	resp, err := (&http.Client{Timeout: 300 * time.Second}).Do(req)
	if err != nil {
		log.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		buf := make([]byte, 600)
		n, _ := resp.Body.Read(buf)
		log.Fatalf("status=%d body=%s", resp.StatusCode, buf[:n])
	}
	fmt.Printf("function=%s status=200\n", *fn)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	reas, cont := 0, 0
	var firstR, firstC float64
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var d struct {
			Response  string `json:"response"`
			Reasoning string `json:"reasoning_content"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &d) != nil {
			continue
		}
		ms := float64(time.Since(t0).Microseconds()) / 1000
		if d.Reasoning != "" {
			reas += len(d.Reasoning)
			if firstR == 0 {
				firstR = ms
			}
		}
		if d.Response != "" {
			cont += len(d.Response)
			if firstC == 0 {
				firstC = ms
			}
		}
	}
	fmt.Fprintf(os.Stderr, "  function=%s 思考=%d字(@%.1fs) 正文=%d字(@%.1fs) 总=%.1fs\n",
		*fn, reas, firstR/1000, cont, firstC/1000, float64(time.Since(t0).Seconds()))
}
