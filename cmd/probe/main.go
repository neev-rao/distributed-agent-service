// probe checks a health endpoint without a shell or curl in the runtime image.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

func main() { os.Exit(run(os.Args[1:], os.Stderr)) }

func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	target := fs.String("url", "http://127.0.0.1:8080/readyz", "health endpoint URL")
	timeout := fs.Duration("timeout", 2*time.Second, "request timeout (positive, at most 5s)")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}
	parsed, err := url.Parse(*target)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || *timeout <= 0 || *timeout > 5*time.Second || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "Invalid probe URL or timeout.")
		return 1
	}
	client := &http.Client{Timeout: *timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(*target)
	if err != nil {
		fmt.Fprintln(stderr, "Health endpoint unavailable.")
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "Health endpoint is not ready.")
		return 1
	}
	return 0
}
