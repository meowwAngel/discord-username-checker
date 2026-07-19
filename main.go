package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/proxy"
)

const (
	ENDPOINT    = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"
	PROXY_FILE  = "proxy.txt"
	LIST_FILE   = "list.txt"
	OUTPUT_FILE = "available.txt"
	CONFIG_FILE = "config.json"
	WORDS_FILE  = "words.txt"
	UNUSED_FILE = "unused.txt" // New file for unused usernames

	// Tuning
	CONCURRENCY          = 10
	RETRIES_PER_NAME     = 3
	REQUEST_TIMEOUT_SECS = 30
	BATCH_FLUSH          = 50

	// Tor settings
	TOR_SOCKS5_ADDR        = "127.0.0.1:9050"
	USE_TOR                = true
	CIRCUIT_SWITCH_AFTER   = 50
	MAX_CONSECUTIVE_FAILS  = 3
	RATE_LIMIT_BACKOFF_MIN = 60
	RATE_LIMIT_BACKOFF_MAX = 300
	MAX_RATE_LIMIT_RETRIES = 3
)

// Colors
const (
	W = "\033[97;1m"
	G = "\033[92;1m"
	R = "\033[91;1m"
	B = "\033[94;1m"
	Y = "\033[93;1m"
	X = "\033[0m"
)

func cAvailable(u string) string {
	return fmt.Sprintf("%s[+] %s%s", G, u, X)
}

func cTaken(u string) string {
	return fmt.Sprintf("%s[-] %s%s", R, u, X)
}

func cInfo(msg string) string {
	return fmt.Sprintf("%s[i]%s %s", W, X, msg)
}

func cError(msg string) string {
	return fmt.Sprintf("%s[!]%s %s", R, X, msg)
}

func cPause(msg string) string {
	return fmt.Sprintf("%s[⏱]%s %s", Y, X, msg)
}

func cCircuit(msg string) string {
	return fmt.Sprintf("%s[🔄]%s %s", B, X, msg)
}

func waitExit() {
	fmt.Printf("\n  Press Enter to exit...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	os.Exit(1)
}

func rgb(r, g, b int) string {
	return fmt.Sprintf("\033[38;2;%d;%d;%dm", r, g, b)
}

func fade(text string, r1, g1, b1, r2, g2, b2 int) string {
	var result string
	runes := []rune(text)
	n := len(runes)
	if n == 0 {
		return ""
	}
	for i, r := range runes {
		ratio := float64(i) / float64(n)
		currR := int(float64(r1) + ratio*float64(r2-r1))
		currG := int(float64(g1) + ratio*float64(g2-g1))
		currB := int(float64(b1) + ratio*float64(b2-b1))
		result += rgb(currR, currG, currB) + string(r)
	}
	return result + X
}

func enableANSIColors() {
	if runtime.GOOS == "windows" {
		return
	}
}

func setTitle(title string) {
	fmt.Printf("\033]0;%s\007", title)
}

type Config struct {
	WebhookURL string `json:"webhook_url"`
}

type DiscordResp struct {
	Taken *bool `json:"taken"`
}

type Stats struct {
	Available       int32
	Taken           int32
	Errors          int32
	Checked         int32
	CircuitSwitches int32
	RateLimits      int32
	BackoffPauses   int32
}

func loadConfig() Config {
	var cfg Config
	f, err := os.Open(CONFIG_FILE)
	if err != nil {
		fmt.Println(cError("config.json not found. Please create it."))
		waitExit()
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		fmt.Println(cError("Failed to parse config.json."))
		waitExit()
	}

	return cfg
}

var usernameRe = regexp.MustCompile(`^[a-z0-9._]{2,32}$`)

func allowedUsername(u string) bool {
	return usernameRe.MatchString(u)
}

func sendWebhook(client *http.Client, webhookURL, username string) {
	if webhookURL == "" {
		return
	}
	payload := map[string]interface{}{
		"embeds": []map[string]interface{}{
			{
				"description": fmt.Sprintf("``%s`` *is * ***available***", username),
				"color":       2829617,
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", webhookURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

// TorController manages Tor circuit switching with backoff
type TorController struct {
	mu                sync.Mutex
	successfulCount   int
	consecutiveFails  int
	circuitID         int
	client            *http.Client
	transport         *http.Transport
	rateLimitAttempts int
	backoffDuration   time.Duration
	lastRateLimitTime time.Time
	requestCount      int
}

// NewTorController creates a new Tor controller
func NewTorController() (*TorController, error) {
	tc := &TorController{
		circuitID:         0,
		successfulCount:   0,
		consecutiveFails:  0,
		rateLimitAttempts: 0,
		backoffDuration:   time.Duration(RATE_LIMIT_BACKOFF_MIN) * time.Second,
		requestCount:      0,
	}
	
	if err := tc.createNewCircuit(); err != nil {
		return nil, err
	}
	
	return tc, nil
}

// createNewCircuit creates a new Tor circuit
func (tc *TorController) createNewCircuit() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	// Close old transport if exists
	if tc.transport != nil {
		tc.transport.CloseIdleConnections()
	}
	
	// Create SOCKS5 dialer
	dialer, err := proxy.SOCKS5("tcp", TOR_SOCKS5_ADDR, nil, proxy.Direct)
	if err != nil {
		return fmt.Errorf("failed to create Tor SOCKS5 dialer: %v", err)
	}
	
	// Create new transport with fresh connections
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.(proxy.ContextDialer).DialContext(ctx, network, addr)
		},
		MaxIdleConnsPerHost: 2,
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
		DisableKeepAlives:   false,
	}
	
	tc.transport = transport
	tc.client = &http.Client{
		Timeout:   time.Duration(REQUEST_TIMEOUT_SECS) * time.Second,
		Transport: transport,
	}
	
	tc.circuitID++
	tc.successfulCount = 0
	tc.consecutiveFails = 0
	tc.rateLimitAttempts = 0
	tc.requestCount = 0
	
	return nil
}

// shouldSwitchCircuit checks if we should switch circuits
func (tc *TorController) shouldSwitchCircuit() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	// Don't switch if we just got rate limited and are in backoff
	if time.Since(tc.lastRateLimitTime) < tc.backoffDuration && !tc.lastRateLimitTime.IsZero() {
		return false
	}
	
	// Switch if we've had too many consecutive failures
	if tc.consecutiveFails >= MAX_CONSECUTIVE_FAILS {
		return true
	}
	
	// Switch if we've done enough successful requests
	if tc.successfulCount >= CIRCUIT_SWITCH_AFTER {
		return true
	}
	
	return false
}

// switchCircuit switches to a new Tor circuit
func (tc *TorController) switchCircuit() error {
	fmt.Printf("%s Switching to new Tor circuit #%d\n", cCircuit(""), tc.circuitID+1)
	return tc.createNewCircuit()
}

// recordSuccess records a successful request
func (tc *TorController) recordSuccess() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.successfulCount++
	tc.consecutiveFails = 0
	tc.requestCount++
}

// recordFailure records a failed request
func (tc *TorController) recordFailure() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.consecutiveFails++
	tc.requestCount++
}

// recordRateLimit handles rate limit with exponential backoff
func (tc *TorController) recordRateLimit() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	tc.rateLimitAttempts++
	
	// Exponential backoff with jitter
	baseBackoff := time.Duration(RATE_LIMIT_BACKOFF_MIN) * time.Second
	// Increase backoff with each attempt, up to max
	multiplier := float64(min(tc.rateLimitAttempts, 10))
	backoff := time.Duration(float64(baseBackoff) * multiplier)
	
	// Add jitter (±20%)
	jitter := time.Duration(rand.Intn(int(backoff)/5)) - time.Duration(int(backoff)/10)
	backoff += jitter
	
	// Cap at max
	if backoff > time.Duration(RATE_LIMIT_BACKOFF_MAX)*time.Second {
		backoff = time.Duration(RATE_LIMIT_BACKOFF_MAX) * time.Second
	}
	
	tc.backoffDuration = backoff
	tc.lastRateLimitTime = time.Now()
	
	fmt.Printf("%s Rate limited! Circuit #%d, attempt %d, backing off for %.0f seconds...\n", 
		cError(""), tc.circuitID, tc.rateLimitAttempts, backoff.Seconds())
}

// shouldRetrySameCircuit checks if we should retry with same circuit
func (tc *TorController) shouldRetrySameCircuit() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.rateLimitAttempts < MAX_RATE_LIMIT_RETRIES
}

// getCircuitInfo returns current circuit info
func (tc *TorController) getCircuitInfo() (int, int, int, int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.circuitID, tc.successfulCount, tc.consecutiveFails, tc.rateLimitAttempts
}

// request performs an HTTP request with circuit management
func (tc *TorController) request(req *http.Request) (*http.Response, error) {
	// Check if we should switch circuits BEFORE making the request
	if tc.shouldSwitchCircuit() {
		if err := tc.switchCircuit(); err != nil {
			return nil, err
		}
	}
	
	resp, err := tc.client.Do(req)
	
	if err != nil {
		tc.recordFailure()
		return resp, err
	}
	
	// Check if we got rate limited
	if resp.StatusCode == 429 || resp.StatusCode == 403 {
		tc.recordRateLimit()
		return resp, nil
	}
	
	// Only count successful (200) responses as successes
	if resp.StatusCode == 200 {
		tc.recordSuccess()
	}
	
	return resp, nil
}

// requestWithCircuitSwitch performs a request with circuit management
func (tc *TorController) requestWithCircuitSwitch(username string) (bool, error) {
	bodyMap := map[string]string{"username": username}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return false, err
	}
	
	req, err := http.NewRequest("POST", ENDPOINT, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/")
	
	resp, err := tc.request(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	
	status := resp.StatusCode
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	
	// Handle rate limiting with retries
	if status == 429 || status == 403 {
		// If we haven't exceeded max retries, wait and retry with same circuit
		if tc.shouldRetrySameCircuit() {
			waitTime := tc.backoffDuration
			fmt.Printf("%s Waiting %.0f seconds before retrying with same circuit...\n", cPause(""), waitTime.Seconds())
			time.Sleep(waitTime)
			
			// Retry the request
			return tc.requestWithCircuitSwitch(username)
		}
		
		// Max retries exceeded, switch circuit
		fmt.Printf("%s Max retries exceeded, switching circuit...\n", cCircuit(""))
		tc.switchCircuit()
		
		// Wait a bit before retrying with new circuit
		time.Sleep(10 * time.Second)
		
		// Retry with new circuit
		return tc.requestWithCircuitSwitch(username)
	}
	
	if status == 200 {
		var dr DiscordResp
		if err := json.Unmarshal(bodyBytes, &dr); err != nil {
			return false, fmt.Errorf("json parse error: %v", err)
		}
		if dr.Taken == nil {
			return false, fmt.Errorf("taken field is nil")
		}
		
		// Reset rate limit attempts on success
		tc.mu.Lock()
		tc.rateLimitAttempts = 0
		tc.mu.Unlock()
		
		return !*dr.Taken, nil
	}
	
	return false, fmt.Errorf("status %d: %s", status, bodyStr)
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Fallback HTTP client for non-Tor requests
func createDirectHTTPClient() *http.Client {
	return &http.Client{
		Timeout: time.Duration(REQUEST_TIMEOUT_SECS) * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: CONCURRENCY * 2,
			MaxIdleConns:        CONCURRENCY * 4,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  false,
		},
	}
}

// appendToFile appends lines to a file
func appendToFile(path string, lines []string) {
	if len(lines) == 0 {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	for _, l := range lines {
		_, _ = f.WriteString(l + "\n")
	}
}

// appendUniqueToFile appends lines to a file, avoiding duplicates using a seen set
func appendUniqueToFile(path string, lines []string, seen map[string]bool) {
	if len(lines) == 0 {
		return
	}
	
	// Read existing file to avoid duplicates
	existing := make(map[string]bool)
	if f, err := os.Open(path); err == nil {
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			existing[strings.TrimSpace(sc.Text())] = true
		}
	}
	
	// Filter out duplicates
	var uniqueLines []string
	for _, l := range lines {
		if !existing[l] && !seen[l] {
			uniqueLines = append(uniqueLines, l)
			seen[l] = true
		}
	}
	
	if len(uniqueLines) == 0 {
		return
	}
	
	// Append unique lines
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	for _, l := range uniqueLines {
		_, _ = f.WriteString(l + "\n")
	}
}

func iterFromList() []string {
	f, err := os.Open(LIST_FILE)
	if err != nil {
		fmt.Println(cInfo(fmt.Sprintf("`%s` not found. Put one username per line.", LIST_FILE)))
		return nil
	}
	defer f.Close()
	seen := make(map[string]struct{})
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		u := strings.TrimSpace(sc.Text())
		if u == "" {
			continue
		}
		if !allowedUsername(u) {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

func sourceFromSlice(slice []string) func(context.Context, chan<- string) {
	return func(ctx context.Context, ch chan<- string) {
		for _, u := range slice {
			select {
			case <-ctx.Done():
				return
			case ch <- u:
			}
		}
	}
}

func all4CharCombos() []string {
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789"
	n := len(alphabet)
	total := n * n * n * n
	combos := make([]string, 0, total)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			for k := 0; k < n; k++ {
				for l := 0; l < n; l++ {
					u := string([]byte{alphabet[i], alphabet[j], alphabet[k], alphabet[l]})
					combos = append(combos, u)
				}
			}
		}
	}
	return combos
}

func all4LetterCombos() []string {
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	n := len(alphabet)
	total := n * n * n * n
	combos := make([]string, 0, total)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			for k := 0; k < n; k++ {
				for l := 0; l < n; l++ {
					u := string([]byte{alphabet[i], alphabet[j], alphabet[k], alphabet[l]})
					combos = append(combos, u)
				}
			}
		}
	}
	return combos
}

func sourceAll4CharRandom() func(context.Context, chan<- string) {
	return func(ctx context.Context, ch chan<- string) {
		fmt.Println(cInfo("Generating 1,679,616 combinations..."))
		combos := all4CharCombos()
		fmt.Println(cInfo("Shuffling..."))
		rand.Shuffle(len(combos), func(i, j int) {
			combos[i], combos[j] = combos[j], combos[i]
		})
		for _, u := range combos {
			select {
			case <-ctx.Done():
				return
			case ch <- u:
			}
		}
	}
}

func sourceAll4LettersRandom() func(context.Context, chan<- string) {
	return func(ctx context.Context, ch chan<- string) {
		fmt.Println(cInfo("Generating 456,976 combinations..."))
		combos := all4LetterCombos()
		fmt.Println(cInfo("Shuffling..."))
		rand.Shuffle(len(combos), func(i, j int) {
			combos[i], combos[j] = combos[j], combos[i]
		})
		for _, u := range combos {
			select {
			case <-ctx.Done():
				return
			case ch <- u:
			}
		}
	}
}

func all3CharCombos() []string {
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789._"
	n := len(alphabet)
	total := n * n * n
	combos := make([]string, 0, total)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			for k := 0; k < n; k++ {
				u := string([]byte{alphabet[i], alphabet[j], alphabet[k]})
				combos = append(combos, u)
			}
		}
	}
	return combos
}

func all3LetterCombos() []string {
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	n := len(alphabet)
	total := n * n * n
	combos := make([]string, 0, total)

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			for k := 0; k < n; k++ {
				u := string([]byte{alphabet[i], alphabet[j], alphabet[k]})
				combos = append(combos, u)
			}
		}
	}
	return combos
}

func sourceAll3CharRandom() func(context.Context, chan<- string) {
	return func(ctx context.Context, ch chan<- string) {
		fmt.Println(cInfo("Generating 54,872 combinations..."))
		combos := all3CharCombos()
		fmt.Println(cInfo("Shuffling..."))
		rand.Shuffle(len(combos), func(i, j int) {
			combos[i], combos[j] = combos[j], combos[i]
		})
		for _, u := range combos {
			select {
			case <-ctx.Done():
				return
			case ch <- u:
			}
		}
	}
}

func sourceAll3LettersRandom() func(context.Context, chan<- string) {
	return func(ctx context.Context, ch chan<- string) {
		fmt.Println(cInfo("Generating 17,576 combinations..."))
		combos := all3LetterCombos()
		fmt.Println(cInfo("Shuffling..."))
		rand.Shuffle(len(combos), func(i, j int) {
			combos[i], combos[j] = combos[j], combos[i]
		})
		for _, u := range combos {
			select {
			case <-ctx.Done():
				return
			case ch <- u:
			}
		}
	}
}

func sourceEnglishWords() func(context.Context, chan<- string) {
	return func(ctx context.Context, ch chan<- string) {
		if _, err := os.Stat(WORDS_FILE); os.IsNotExist(err) {
			fmt.Println(cInfo("Downloading English wordlist (approx 4MB)..."))
			client := &http.Client{Timeout: 60 * time.Second}
			resp, err := client.Get("https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt")
			if err != nil {
				fmt.Println(cError("Error downloading wordlist: " + err.Error()))
				return
			}
			defer resp.Body.Close()

			f, err := os.Create(WORDS_FILE)
			if err != nil {
				fmt.Println(cError("Error creating words.txt: " + err.Error()))
				return
			}
			_, _ = io.Copy(f, resp.Body)
			f.Close()
			fmt.Println(cInfo("Download complete."))
		}

		f, err := os.Open(WORDS_FILE)
		if err != nil {
			fmt.Println(cError("Error opening words.txt: " + err.Error()))
			return
		}
		defer f.Close()

		sc := bufio.NewScanner(f)
		for sc.Scan() {
			u := strings.ToLower(strings.TrimSpace(sc.Text()))
			if u == "" || !allowedUsername(u) {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- u:
			}
		}
	}
}

func runChecker(usernameSource func(context.Context, chan<- string), webhookURL string) {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	jobs := make(chan string, CONCURRENCY*4)
	var wg sync.WaitGroup

	var stats Stats
	var outMu sync.Mutex
	availBuf := make([]string, 0, BATCH_FLUSH)
	unusedBuf := make([]string, 0, BATCH_FLUSH)
	
	// Track seen usernames to avoid duplicates in unused.txt
	unusedSeen := make(map[string]bool)
	var unusedMu sync.Mutex

	// Initialize Tor controller
	var torController *TorController
	var err error
	if USE_TOR {
		torController, err = NewTorController()
		if err != nil {
			fmt.Println(cError("Failed to initialize Tor: " + err.Error()))
			fmt.Println(cInfo("Falling back to direct connection..."))
			torController = nil
		} else {
			fmt.Println(cInfo("Tor initialized successfully!"))
			circuitID, successCount, failCount, rateAttempts := torController.getCircuitInfo()
			fmt.Printf(cInfo("Circuit #%d started (successes: %d, failures: %d, rate-limits: %d)\n"), 
				circuitID, successCount, failCount, rateAttempts)
		}
	}

	webhookClient := &http.Client{
		Timeout: time.Duration(REQUEST_TIMEOUT_SECS) * time.Second,
	}

	// Update Title thread
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				torStatus := "OFF"
				if USE_TOR && torController != nil {
					circuitID, successCount, failCount, rateAttempts := torController.getCircuitInfo()
					torStatus = fmt.Sprintf("ON (c#%d s:%d f:%d r:%d)", circuitID, successCount, failCount, rateAttempts)
				} else if USE_TOR && torController == nil {
					torStatus = "FAILED"
				}
				setTitle(fmt.Sprintf("xpniped [Tor:%s] | Avail: %d | Taken: %d | Err: %d | Checked: %d | Switches: %d | RateLimits: %d",
					torStatus, stats.Available, stats.Taken, stats.Errors, stats.Checked, stats.CircuitSwitches, stats.RateLimits))
			}
		}
	}()

	// Periodic flush for unused usernames
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				unusedMu.Lock()
				if len(unusedBuf) > 0 {
					appendUniqueToFile(UNUSED_FILE, unusedBuf, unusedSeen)
					fmt.Printf("%s Flushed %d unused usernames to %s\n", cInfo(""), len(unusedBuf), UNUSED_FILE)
					unusedBuf = unusedBuf[:0]
				}
				unusedMu.Unlock()
			}
		}
	}()

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case u, ok := <-jobs:
				if !ok {
					return
				}

				attempt := 0
				var available bool
				var err error
				var success bool

				for attempt < RETRIES_PER_NAME && !success {
					attempt++
					
					if USE_TOR && torController != nil {
						available, err = torController.requestWithCircuitSwitch(u)
						
						if err != nil && strings.Contains(err.Error(), "rate limited") {
							atomic.AddInt32(&stats.RateLimits, 1)
							atomic.AddInt32(&stats.CircuitSwitches, 1)
						}
					} else {
						// Fallback to direct connection
						available, err = checkOnceDirect(u)
					}
					
					if err != nil {
						if attempt == RETRIES_PER_NAME {
							atomic.AddInt32(&stats.Errors, 1)
							fmt.Printf("%s %s - %s\n", cError("Failed"), u, err.Error())
						}
						// Wait between retries
						if attempt < RETRIES_PER_NAME {
							time.Sleep(2 * time.Second)
						}
					} else {
						success = true
					}
				}

				if !success {
					continue
				}

				atomic.AddInt32(&stats.Checked, 1)

				if available {
					atomic.AddInt32(&stats.Available, 1)
					outMu.Lock()

					fmt.Println(cAvailable(u))
					availBuf = append(availBuf, u)
					if len(availBuf) >= BATCH_FLUSH {
						appendToFile(OUTPUT_FILE, availBuf)
						availBuf = availBuf[:0]
					}
					outMu.Unlock()

					if webhookURL != "" {
						go sendWebhook(webhookClient, webhookURL, u)
					}
				} else {
					atomic.AddInt32(&stats.Taken, 1)
					fmt.Println(cTaken(u))
					
					// Log unused (taken) usernames to unused.txt
					unusedMu.Lock()
					unusedBuf = append(unusedBuf, u)
					if len(unusedBuf) >= BATCH_FLUSH {
						appendUniqueToFile(UNUSED_FILE, unusedBuf, unusedSeen)
						unusedBuf = unusedBuf[:0]
					}
					unusedMu.Unlock()
				}

			}
		}
	}

	for i := 0; i < CONCURRENCY; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		defer close(jobs)
		usernameSource(ctx, jobs)
	}()

	wg.Wait()

	// Final flush
	outMu.Lock()
	if len(availBuf) > 0 {
		appendToFile(OUTPUT_FILE, availBuf)
	}
	outMu.Unlock()
	
	unusedMu.Lock()
	if len(unusedBuf) > 0 {
		appendUniqueToFile(UNUSED_FILE, unusedBuf, unusedSeen)
	}
	unusedMu.Unlock()
}

// checkOnceDirect is the original direct HTTP check (fallback when Tor is unavailable)
func checkOnceDirect(username string) (bool, error) {
	bodyMap := map[string]string{"username": username}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return false, err
	}

	client := createDirectHTTPClient()

	req, err := http.NewRequest("POST", ENDPOINT, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/")

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	if status == 200 {
		var dr DiscordResp
		if err := json.Unmarshal(bodyBytes, &dr); err != nil {
			return false, fmt.Errorf("json parse error: %v", err)
		}
		if dr.Taken == nil {
			return false, fmt.Errorf("taken field is nil")
		}
		return !*dr.Taken, nil
	}

	return false, fmt.Errorf("status %d: %s", status, bodyStr)
}

func main() {

	enableANSIColors()
	rand.Seed(time.Now().UnixNano())

	cfg := loadConfig()
	webhookURL := strings.TrimSpace(cfg.WebhookURL)

	fmt.Println()
	banner := `
  ___    ___ ________  ________  ________   ___  ________  _______   ________     
 |\  \  /  /|\   __  \|\   ____\|\   ___  \|\  \|\   __  \|\  ___ \ |\   ___ \    
 \ \  \/  / | \  \|\  \ \  \___|\ \  \\ \  \ \  \ \  \|\  \ \   __/|\ \  \_|\ \   
  \ \    / / \ \   ____\ \_____  \ \  \\ \  \ \  \ \   ____\ \  \_|/_\ \  \ \\ \  
   /     \/   \ \  \___|\|____|\  \ \  \\ \  \ \  \ \  \___|\ \  \_|\ \ \  \_\\ \ 
  /  /\   \    \ \__\     ____\_\  \ \__\\ \__\ \__\ \__\    \ \_______\ \_______\
 /__/ /\ __\    \|__|    |\_________\|__| \|__|\|__|\|__|     \|_______|\|_______|
 |__|/ \|__|             \|_________|                                             `

	lines := strings.Split(banner, "\n")
	for _, line := range lines {
		fmt.Println("  " + fade(line, 0, 255, 128, 255, 255, 255))
	}

	fmt.Println("\n  " + rgb(100, 100, 100) + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" + X)
	fmt.Printf("                                     %sdev by @ykgtteh%s\n", rgb(150, 150, 150), X)

	// Display Tor status with circuit switching info
	if USE_TOR {
		fmt.Printf("\n  %s[Tor]%s Routing all requests through Tor SOCKS5 proxy (%s)\n", G, X, TOR_SOCKS5_ADDR)
		fmt.Printf("  %s[🔄]%s Circuit switching: after %d successes or %d failures\n", B, X, CIRCUIT_SWITCH_AFTER, MAX_CONSECUTIVE_FAILS)
		fmt.Printf("  %s[⏱]%s Rate limit backoff: %d-%d seconds (exponential)\n", Y, X, RATE_LIMIT_BACKOFF_MIN, RATE_LIMIT_BACKOFF_MAX)
		fmt.Printf("  %s[i]%s Will retry same circuit up to %d times before switching\n", W, X, MAX_RATE_LIMIT_RETRIES)
		fmt.Printf("  %s[i]%s Make sure Tor is running! (sudo systemctl start tor)\n", W, X)
		fmt.Printf("  %s[📝]%s Available usernames -> %s | Unused (taken) -> %s\n", G, X, OUTPUT_FILE, UNUSED_FILE)
	} else {
		fmt.Printf("\n  %s[!]%s Tor is DISABLED - using direct connections\n", R, X)
	}

	fmt.Printf("\n  %s Select Mode:%s\n", rgb(0, 255, 128), X)

	options := []string{
		"Check from list.txt",
		"Check ALL 4-char [a-z0-9]",
		"Check ONLY 4-letter [a-z]",
		"English Words Mode",
		"Check ALL 3-char [a-z0-9._]",
		"Check ONLY 3-letter [a-z]",
	}

	for i, opt := range options {
		fmt.Printf("  %s[%d]%s %-30s", rgb(0, 255, 128), i+1, X, opt)
		if (i+1)%2 == 0 {
			fmt.Println()
		}
	}

	fmt.Printf("\n\n  %s choice > %s", rgb(0, 255, 128), X)

	var choice string
	fmt.Scanln(&choice)

	fmt.Println()
	fmt.Println(G + " ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" + X)
	fmt.Println()

	switch choice {
	case "1":
		usernames := iterFromList()
		if len(usernames) == 0 {
			fmt.Println(cInfo("No valid usernames found in list.txt"))
			return
		}
		runChecker(sourceFromSlice(usernames), webhookURL)
	case "2":
		fmt.Println(cInfo("This will stream through 1,679,616 combos in random order. You can Ctrl+C anytime."))
		runChecker(sourceAll4CharRandom(), webhookURL)
	case "3":
		fmt.Println(cInfo("This will stream through 456,976 combos in random order. You can Ctrl+C anytime."))
		runChecker(sourceAll4LettersRandom(), webhookURL)
	case "4":
		fmt.Println(cInfo("Loading English dictionary. This might take a second..."))
		runChecker(sourceEnglishWords(), webhookURL)
	case "5":
		fmt.Println(cInfo("This will stream through 54,872 combos in random order. You can Ctrl+C anytime."))
		runChecker(sourceAll3CharRandom(), webhookURL)
	case "6":
		fmt.Println(cInfo("This will stream through 17,576 combos in random order. You can Ctrl+C anytime."))
		runChecker(sourceAll3LettersRandom(), webhookURL)
	default:
		fmt.Println(cInfo("Invalid choice, defaulting to option 1 (list.txt)."))
		usernames := iterFromList()
		if len(usernames) == 0 {
			fmt.Println(cInfo("No valid usernames found in list.txt"))
			return
		}
		runChecker(sourceFromSlice(usernames), webhookURL)
	}
}
