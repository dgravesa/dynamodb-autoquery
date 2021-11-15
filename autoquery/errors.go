package autoquery

import "fmt"

// ErrParsingComplete is returned by Parser.Next when all query items have been returned or when
// max pagination has been reached.
type ErrParsingComplete struct {
	reason string
}

func (e ErrParsingComplete) Error() string {
	return fmt.Sprintf("parsing complete: %s", e.reason)
}
