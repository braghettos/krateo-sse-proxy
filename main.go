// Package main implements a thin SSE proxy that polls ClickHouse for new
// Kubernetes events and broadcasts them to connected browser clients using
// the Server-Sent Events (SSE) protocol.
//
// The Krateo frontend EventList widget connects to /notifications/ and listens
// for SSE messages whose `event:` field matches the compositionId. Each SSE
// message `data:` field contains a single SSEK8sEvent JSON object.
//
// No external dependencies – only the Go standard library is used.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// SSEK8sEvent – the JSON structure expected by the Krateo frontend EventList.
// ---------------------------------------------------------------------------

// SSEK8sEvent matches the TypeScript interface in the Krateo frontend.
type SSEK8sEvent struct {
	Metadata struct {
		Name              string `json:"name"`
		Namespace         string `json:"namespace"`
		UID               string `json:"uid"`
		CreationTimestamp string `json:"creationTimestamp"`
	} `json:"metadata"`
	InvolvedObject struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		Namespace  string `json:"namespace"`
		UID        string `json:"uid"`
	} `json:"involvedObject"`
	Reason        string `json:"reason"`
	Message       string `json:"message"`
	Type          string `json:"type"` // "Normal" | "Warning"
	FirstTimestamp string `json:"firstTimestamp"`
	LastTimestamp  string `json:"lastTimestamp"`
	EventTime      string `json:"eventTime"`
	Source         struct {
		Component string `json:"component"`
	} `json:"source"`
}

// ---------------------------------------------------------------------------
// chRow – a row returned by the ClickHouse polling query (JSONEachRow format).
// Fields are extracted from the raw K8s event JSON stored in otel_logs.Body
// by the k8sobjects receiver, and the composition ID is enriched by the
// compositionresolver OTel processor.
// ---------------------------------------------------------------------------

type chRow struct {
	TsUnix          int64  `json:"ts_unix"`
	CompositionID   string `json:"composition_id"`
	ObjAPIVersion   string `json:"obj_apiversion"`
	ObjName         string `json:"obj_name"`
	ObjNamespace    string `json:"obj_namespace"`
	ObjUID          string `json:"obj_uid"`
	ObjKind         string `json:"obj_kind"`
	Reason          string `json:"reason"`
	Message         string `json:"message"`
	Type            string `json:"type"`
	EventTime       string `json:"event_time"`
	SourceComponent string `json:"source_component"`
}

func (row chRow) toSSEK8sEvent() SSEK8sEvent {
	var evt SSEK8sEvent
	evt.Metadata.Name = row.ObjName
	evt.Metadata.Namespace = row.ObjNamespace
	evt.Metadata.UID = row.ObjUID
	evt.Metadata.CreationTimestamp = row.EventTime
	evt.InvolvedObject.APIVersion = row.ObjAPIVersion
	evt.InvolvedObject.Kind = row.ObjKind
	evt.InvolvedObject.Name = row.ObjName
	evt.InvolvedObject.Namespace = row.ObjNamespace
	evt.InvolvedObject.UID = row.ObjUID
	evt.Reason = row.Reason
	evt.Message = row.Message
	evt.Type = row.Type
	evt.FirstTimestamp = row.EventTime
	evt.LastTimestamp = row.EventTime
	evt.EventTime = row.EventTime
	evt.Source.Component = row.SourceComponent
	return evt
}

// ---------------------------------------------------------------------------
// Hub – fan-out SSE messages to all connected clients.
// ---------------------------------------------------------------------------

type sseMessage struct {
	topic string
	data  []byte
}

type client struct {
	ch chan sseMessage
}

type hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
}

func newHub() *hub {
	return &hub{clients: make(map[*client]struct{})}
}

func (h *hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	close(c.ch)
}

func (h *hub) broadcast(msg sseMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		// Non-blocking send: drop the message for slow clients rather than blocking.
		select {
		case c.ch <- msg:
		default:
		}
	}
}

func (h *hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type config struct {
	clickhouseURL      string
	clickhouseUser     string
	clickhousePassword string
	listenAddr         string
}

func loadConfig() config {
	return config{
		clickhouseURL:      getEnv("CLICKHOUSE_URL", "http://krateo-clickstack-clickhouse.clickhouse-system.svc:8123"),
		clickhouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		clickhousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		listenAddr:         getEnv("LISTEN_ADDR", ":8080"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---------------------------------------------------------------------------
// Poller – periodically queries ClickHouse and broadcasts new events.
// ---------------------------------------------------------------------------

// pollSQL fetches K8s events from otel_logs that arrived after lastSeenUnix.
// Events are stored as raw JSON in Body by the k8sobjects receiver.
// The compositionresolver processor enriches LogAttributes with krateo.io/composition-id.
// %%Y, %%m etc. become %Y, %m after fmt.Sprintf substitutes %d for the timestamp.
const pollSQL = `SELECT
    toUnixTimestamp(Timestamp)                                                      AS ts_unix,
    ifNull(LogAttributes['krateo.io/composition-id'], '')                           AS composition_id,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'apiVersion'), '')   AS obj_apiversion,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'name'), '')         AS obj_name,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'namespace'), '')    AS obj_namespace,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'uid'), '')          AS obj_uid,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'kind'), '')         AS obj_kind,
    ifNull(JSONExtractString(Body, 'object', 'reason'), '')                         AS reason,
    ifNull(JSONExtractString(Body, 'object', 'message'), '')                        AS message,
    ifNull(JSONExtractString(Body, 'object', 'type'), 'Normal')                     AS type,
    coalesce(
        nullIf(JSONExtractString(Body, 'object', 'eventTime'), ''),
        nullIf(JSONExtractString(Body, 'object', 'lastTimestamp'), ''),
        formatDateTime(toDateTime(Timestamp), '%%Y-%%m-%%dT%%H:%%i:%%SZ', 'UTC')
    )                                                                                AS event_time,
    ifNull(JSONExtractString(Body, 'object', 'source', 'component'), '')            AS source_component
FROM otel_logs
WHERE ResourceAttributes['telemetry.source'] = 'k8s-events'
  AND JSONExtractString(Body, 'object', 'reason') != ''
  AND toUnixTimestamp(Timestamp) > %d
ORDER BY Timestamp ASC
LIMIT 500
FORMAT JSONEachRow`

type poller struct {
	cfg          config
	hub          *hub
	lastSeenUnix int64
}

func newPoller(cfg config, h *hub) *poller {
	// Initialise to one hour ago to surface recent events on startup.
	return &poller{
		cfg:          cfg,
		hub:          h,
		lastSeenUnix: time.Now().Add(-1 * time.Hour).Unix(),
	}
}

func (p *poller) run(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.hub.count() == 0 {
				// No clients connected – skip the poll to avoid unnecessary load.
				continue
			}
			p.poll()
		}
	}
}

func (p *poller) poll() {
	query := fmt.Sprintf(pollSQL, p.lastSeenUnix)
	rows, err := p.queryClickHouse(query)
	if err != nil {
		log.Printf("[poller] query error: %v", err)
		return
	}

	maxTs := p.lastSeenUnix
	for _, row := range rows {
		if row.TsUnix > maxTs {
			maxTs = row.TsUnix
		}

		evt := row.toSSEK8sEvent()
		data, err := json.Marshal(evt)
		if err != nil {
			log.Printf("[poller] marshal error: %v", err)
			continue
		}

		// Global topic — mirrors eventsse behaviour where all events
		// are published under the "krateo" topic.
		p.hub.broadcast(sseMessage{topic: "krateo", data: data})

		// Composition-specific topic so per-composition listeners
		// only receive their own events.
		if row.CompositionID != "" {
			p.hub.broadcast(sseMessage{topic: row.CompositionID, data: data})
		}
	}

	if maxTs > p.lastSeenUnix {
		p.lastSeenUnix = maxTs
	}
	if len(rows) > 0 {
		log.Printf("[poller] broadcasted %d event(s), lastSeen=%d", len(rows), p.lastSeenUnix)
	}
}

func (p *poller) queryClickHouse(query string) ([]chRow, error) {
	req, err := http.NewRequest(http.MethodPost, p.cfg.clickhouseURL, strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")
	if p.cfg.clickhouseUser != "" {
		req.SetBasicAuth(p.cfg.clickhouseUser, p.cfg.clickhousePassword)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse returned %d: %s", resp.StatusCode, body)
	}

	// FORMAT JSONEachRow: one JSON object per newline.
	var rows []chRow
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var row chRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			log.Printf("[poller] unmarshal error: %v (line=%s)", err, line)
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

func handleSSE(h *hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

		c := &client{ch: make(chan sseMessage, 64)}
		h.register(c)
		defer h.unregister(c)

		// Initial comment confirms the connection to the browser.
		fmt.Fprintf(w, ": connected\n\n")
		flusher.Flush()

		keepalive := time.NewTicker(25 * time.Second)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-keepalive.C:
				// SSE comment as a keepalive ping to prevent proxy timeouts.
				fmt.Fprintf(w, ": keepalive\n\n")
				flusher.Flush()
			case msg, ok := <-c.ch:
				if !ok {
					return
				}
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.topic, msg.data)
				flusher.Flush()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	cfg := loadConfig()
	h := newHub()
	p := newPoller(cfg, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.run(ctx)

	mux := http.NewServeMux()
	// Accept both /notifications and /notifications/ to match frontend behaviour.
	mux.HandleFunc("/notifications/", handleSSE(h))
	mux.HandleFunc("/notifications", handleSSE(h))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	log.Printf("[sse-proxy] listening on %s", cfg.listenAddr)
	if err := http.ListenAndServe(cfg.listenAddr, mux); err != nil {
		log.Fatalf("[sse-proxy] fatal: %v", err)
	}
}
