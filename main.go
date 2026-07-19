package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ENDPOINT    = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"
	PROXY_FILE  = "proxy.txt"
	LIST_FILE   = "list.txt"
	OUTPUT_FILE = "available.txt"
	CONFIG_FILE = "config.json"
	WORDS_FILE  = "words.txt"

	// Conservative tuning to avoid rate limiting
	CONCURRENCY          = 10            // Very low to avoid rate limiting
	RETRIES_PER_NAME     = 3
	REQUEST_TIMEOUT_SECS = 20
	BATCH_FLUSH          = 50
	BACKOFF_BASE         = 1.0          // Start with 1 second backoff
	MAX_BACKOFF          = 30.0         // Max backoff of 30 seconds
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

func cDebug(msg string) string {
	return fmt.Sprintf("%s[debug]%s %s", Y, X, msg)
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
	Available int32
	Taken     int32
	Errors    int32
	Checked   int32
	RateLimit int32
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

func checkOnce(username string) (bool, error) {
	bodyMap := map[string]string{"username": username}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return false, err
	}
	
	tr := &http.Transport{
		MaxIdleConnsPerHost: 5,
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
	}
	
	client := &http.Client{
		Timeout:   time.Duration(REQUEST_TIMEOUT_SECS) * time.Second,
		Transport: tr,
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
	
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	
	status := resp.StatusCode
	
	// Read body for debugging
	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	
	// Debug output for non-200 responses
	if status != 200 {
		fmt.Printf("%s %s (status %d): %s\n", cDebug("Response"), username, status, bodyStr)
	}
	
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
	
	// Handle different status codes
	switch status {
	case 429:
		return false, fmt.Errorf("RATE_LIMITED")
	case 403:
		return false, fmt.Errorf("FORBIDDEN")
	case 401:
		return false, fmt.Errorf("UNAUTHORIZED")
	case 500, 502, 503, 504:
		return false, fmt.Errorf("SERVER_ERROR")
	default:
		return false, fmt.Errorf("status %d: %s", status, bodyStr)
	}
}

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
			resp, err := http.Get("https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt")
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
	
	// Global rate limit state
	var rateLimitUntil int64
	var rateLimitMu sync.Mutex

	webhookClient := &http.Client{
		Timeout: time.Duration(REQUEST_TIMEOUT_SECS) * time.Second,
	}

	// Update Title thread
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				setTitle(fmt.Sprintf("xpniped | Avail: %d | Taken: %d | Err: %d | Checked: %d | RL: %d",
					stats.Available, stats.Taken, stats.Errors, stats.Checked, stats.RateLimit))
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

				// Check if we're globally rate limited
				rateLimitMu.Lock()
				if time.Now().Unix() < rateLimitUntil {
					waitTime := time.Duration(rateLimitUntil-time.Now().Unix()) * time.Second
					rateLimitMu.Unlock()
					fmt.Printf("%s Rate limited, waiting %v...\n", cDebug(""), waitTime)
					time.Sleep(waitTime)
					// Put the username back in the queue
					select {
					case jobs <- u:
						continue
					case <-ctx.Done():
						return
					}
				}
				rateLimitMu.Unlock()

				attempt := 0
				var available bool
				var err error
				var success bool
				
				for attempt < RETRIES_PER_NAME && !success {
					attempt++
					available, err = checkOnce(u)
					if err != nil {
						errMsg := err.Error()
						
						// Handle rate limiting
						if strings.Contains(errMsg, "RATE_LIMITED") {
							atomic.AddInt32(&stats.RateLimit, 1)
							rateLimitMu.Lock()
							// Wait 30 seconds when rate limited
							rateLimitUntil = time.Now().Unix() + 30
							rateLimitMu.Unlock()
							
							// Put username back in queue
							time.Sleep(5 * time.Second)
							select {
							case jobs <- u:
								continue
							case <-ctx.Done():
								return
							}
						} else if strings.Contains(errMsg, "FORBIDDEN") || strings.Contains(errMsg, "UNAUTHORIZED") {
							// These are permanent errors, don't retry
							atomic.AddInt32(&stats.Errors, 1)
							fmt.Printf("%s %s - %s\n", cError("Error checking"), u, errMsg)
							success = false
							break
						}
						
						if attempt == RETRIES_PER_NAME {
							atomic.AddInt32(&stats.Errors, 1)
							if !strings.Contains(errMsg, "RATE_LIMITED") {
								fmt.Printf("%s %s - %s (after %d attempts)\n", cError("Failed"), u, errMsg, attempt)
							}
						}
						
						// Exponential backoff
						backoff := math.Min(BACKOFF_BASE*math.Pow(2.0, float64(attempt-1)), MAX_BACKOFF)
						backoff += rand.Float64() * 0.5
						time.Sleep(time.Duration(backoff * float64(time.Second)))
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
				}
				
				// Add a small delay between requests to avoid rate limiting
				time.Sleep(200 * time.Millisecond)

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

	outMu.Lock()
	if len(availBuf) > 0 {
		appendToFile(OUTPUT_FILE, availBuf)
	}
	outMu.Unlock()
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
