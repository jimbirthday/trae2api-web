// probe 调试工具：直连上游打印原始 SSE 事件，用于诊断流式"不丝滑"。
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/upstream"
)

func main() {
	dir := flag.String("auths", "./auths", "auth dir")
	model := flag.String("model", "glm-5.2", "model")
	prompt := flag.String("p", "只回复两个字：好的", "prompt")
	extra := flag.String("extra", "", "extra JSON fields merged into the request body")
	flag.Parse()
	if flag.Arg(0) == "raw" {
		runRaw(flag.Args()[1:])
		return
	}

	auths, err := auth.LoadDir(*dir)
	if err != nil || len(auths) == 0 {
		log.Fatalf("load auths: %v (%d)", err, len(auths))
	}
	a := auths[0]
	req := map[string]any{
		"model":    *model,
		"messages": []any{map[string]any{"role": "user", "content": *prompt}},
		"stream":   true,
	}
	if *extra != "" {
		var add map[string]any
		if err := json.Unmarshal([]byte(*extra), &add); err != nil {
			log.Fatalf("extra json: %v", err)
		}
		for k, v := range add {
			req[k] = v
		}
	}
	body, _ := json.Marshal(req)
	c := upstream.New()
	t0 := time.Now()
	rc, status, respBody, uerr, terr := c.ChatStreamDetailed(nil, a, body)
	if terr != nil {
		log.Fatalf("transport: %v", terr)
	}
	if status != 200 {
		log.Fatalf("status=%d body=%s uerr=%v", status, respBody, uerr)
	}
	defer rc.Close()
	br := bufio.NewReaderSize(rc, 64*1024)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	prev := t0
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			now := time.Now()
			ev := strings.TrimRight(line, "\r\n")
			if len(ev) > 160 {
				ev = ev[:160] + "…"
			}
			fmt.Fprintf(out, "%8.1fms gap=%6.1fms %s\n",
				float64(now.Sub(t0).Microseconds())/1000,
				float64(now.Sub(prev).Microseconds())/1000, ev)
			out.Flush()
			prev = now
		}
		if err != nil {
			fmt.Fprintf(out, "EOF %v after %.0fms\n", err, float64(time.Since(t0).Microseconds())/1000)
			break
		}
	}
}
