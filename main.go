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

	// Tuning - Adjusted for Arch Linux compatibility
	CONCURRENCY          = 200           // Reduced for better stability on Arch
	RETRIES_PER_NAME     = 4
	REQUEST_TIMEOUT_SECS = 15           // Increased timeout for better reliability
	BATCH_FLUSH          = 50
	BACKOFF_BASE         = 0.25         // Increased backoff for rate limiting
)

// Colors - Using standard ANSI codes compatible with Arch Linux terminals
const (
	W = "\033[97;1m" // White
	G = "\033[92;1m" // Green
	R = "\033[91;1m" // Red
	B = "\033[94;1m" // Blue
	X = "\033[0m"    // Reset
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

// Linux/Unix compatibility - no Windows fixes needed for Arch
func enableANSIColors() {
	// Arch Linux uses modern terminals that support ANSI colors by default
	// No special handling needed
	if runtime.GOOS == "windows" {
		// This code will never run on Arch Linux, but kept for reference
		return
	}
}

func setTitle(title string) {
	// Arch Linux typically uses terminal emulators that support OSC escape sequences
	fmt.Printf("\033]0;%s\007", title)
}

// Data types
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
}

func loadConfig() Config {
	var cfg Config
	f, err := os.Open(CONFIG_FILE)
	if err != nil {
		fmt.Println(rgb(255, 0, 0) + " [!] Error: config.json not found. Please create it." + X)
		waitExit()
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		fmt.Println(rgb(255, 0, 0) + " [!] Error: Failed to parse config.json." + X)
		waitExit()
	}

	return cfg
}

func loadProxies() []string {
	// Proxies are disabled - return empty slice
	return []string{}
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

func checkOnce(username string) *bool {
	bodyMap := map[string]string{"username": username}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return nil
	}
	
	// Updated transport settings for better Linux compatibility - no proxies
	tr := &http.Transport{
		MaxIdleConnsPerHost: CONCURRENCY * 2,
		MaxIdleConns:        CONCURRENCY * 4,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  false,
	}
	
	client := &http.Client{
		Timeout:   time.Duration(REQUEST_TIMEOUT_SECS) * time.Second,
		Transport: tr,
	}
	
	req, err := http.NewRequest("POST", ENDPOINT, bytes.NewReader(body))
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/")
	
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	
	status := resp.StatusCode
	if status == 200 {
		var dr DiscordResp
		if err := json.NewDecoder(resp.Body).Decode(&dr); err != nil {
			return nil
		}
		if dr.Taken == nil {
			return nil
		}
		available := !*dr.Taken
		return &available
	}
	
	// Handle rate limiting more gracefully
	switch status {
	case 401, 403, 409, 429, 500, 502, 520, 521, 522, 523:
		return nil
	default:
		return nil
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

// List iteration
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

// 4char generation
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

// 4letter generation
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

// 3char generation
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

// generate ALL 3-letter [a-z] combos, then shuffle
func all3LetterCombos() []string {
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	n := len(alphabet)
	total := n * n * n // 26^3 = 17,576
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

// random order source for all 3-char
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

// random order source for all 3-letter
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
				fmt.Println(cInfo("Error downloading wordlist: " + err.Error()))
				return
			}
			defer resp.Body.Close()

			f, err := os.Create(WORDS_FILE)
			if err != nil {
				fmt.Println(cInfo("Error creating words.txt: " + err.Error()))
				return
			}
			_, _ = io.Copy(f, resp.Body)
			f.Close()
			fmt.Println(cInfo("Download complete."))
		}

		f, err := os.Open(WORDS_FILE)
		if err != nil {
			fmt.Println(cInfo("Error opening words.txt: " + err.Error()))
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

// Core logic
func runChecker(usernameSource func(context.Context, chan<- string), webhookURL string) {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	jobs := make(chan string, CONCURRENCY*4)
	var wg sync.WaitGroup

	var stats Stats
	var outMu sync.Mutex
	availBuf := make([]string, 0, BATCH_FLUSH)

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
				setTitle(fmt.Sprintf("xpniped | Avail: %d | Taken: %d | Err: %d",
					stats.Available, stats.Taken, stats.Errors))
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
				var result *bool
				for attempt < RETRIES_PER_NAME && result == nil {
					attempt++
					result = checkOnce(u)
					if result == nil {
						if attempt == RETRIES_PER_NAME {
							atomic.AddInt32(&stats.Errors, 1)
						}
						backoff := BACKOFF_BASE*math.Pow(1.5, float64(attempt-1)) + rand.Float64()*0.15
						time.Sleep(time.Duration(backoff * float64(time.Second)))
					}
				}

				atomic.AddInt32(&stats.Checked, 1)

				if result == nil {
					continue
				}

				if *result {
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
					outMu.Lock()
					fmt.Println(cTaken(u))
					outMu.Unlock()
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

	outMu.Lock()
	if len(availBuf) > 0 {
		appendToFile(OUTPUT_FILE, availBuf)
	}
	outMu.Unlock()
}

// Main entry
func main() {

	enableANSIColors()
	rand.Seed(time.Now().UnixNano())

	cfg := loadConfig()
	webhookURL := strings.TrimSpace(cfg.WebhookURL)
	
	// Proxy check removed - proxies are disabled
	
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
		// Gradient from Green (0, 255, 0) to White (255, 255, 255)
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
