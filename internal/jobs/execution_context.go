package jobs

import "context"

// ExecutionMetadata correlates executor events with the durable ownership
// attempt. It contains identifiers only, never prompts or provider credentials.
type ExecutionMetadata struct {
	JobID        string
	AttemptToken string
	Attempt      int
}

type executionMetadataKey struct{}

func WithExecutionMetadata(ctx context.Context, metadata ExecutionMetadata) context.Context {
	return context.WithValue(ctx, executionMetadataKey{}, metadata)
}

func MetadataFromContext(ctx context.Context) (ExecutionMetadata, bool) {
	metadata, ok := ctx.Value(executionMetadataKey{}).(ExecutionMetadata)
	return metadata, ok
}
