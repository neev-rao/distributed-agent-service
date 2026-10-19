package worker

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type classifiedFailure struct{ failure jobs.JobError }

func (e classifiedFailure) Error() string          { return "private provider detail" }
func (e classifiedFailure) Failure() jobs.JobError { return e.failure }

func TestExecutorFailureRepresentation(t *testing.T) {
	want := jobs.JobError{Code: "provider_failure", Message: "Model provider request failed."}
	got := executionFailure(fmt.Errorf("wrapped: %w", classifiedFailure{want}))
	if *got != want {
		t.Fatalf("classified failure lost: %+v", got)
	}
	for _, err := range []error{
		errors.New("private provider detail"),
		classifiedFailure{jobs.JobError{Code: "invalid code", Message: "invalid"}},
		classifiedFailure{jobs.JobError{Code: "valid", Message: strings.Repeat("x", 257)}},
	} {
		got := executionFailure(err)
		if got.Code != "execution_failed" || strings.Contains(got.Message, "private") {
			t.Fatalf("unsafe failure exposed: %+v", got)
		}
	}
}
