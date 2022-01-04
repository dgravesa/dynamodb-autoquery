package autoquery

import (
	"encoding/json"
	"fmt"
)

// ErrParsingComplete is returned by Parser.Next when all query items have been returned or when
// max pagination has been reached.
type ErrParsingComplete struct {
	reason string
}

func (e ErrParsingComplete) Error() string {
	return fmt.Sprintf("parsing complete: %s", e.reason)
}

// ErrNoViableIndexes is returned by Client.Query when no table indexes are usable for the
// requested expression. The string returned by ErrNoViableIndexes.Error includes reasons why each
// index is considered non-viable.
type ErrNoViableIndexes struct {
	IndexErrs []*ErrIndexNotViable
}

func (e ErrNoViableIndexes) Error() string {
	if e.IndexErrs == nil {
		return "no viable indexes found for expression"
	}
	bytes, _ := json.Marshal(e.IndexErrs)
	reasonsPerIndex := string(bytes)
	return fmt.Sprintf("no viable indexes found for expression: %s", reasonsPerIndex)
}

// ErrIndexNotViable is returned when a specified index is not usable for the requested
// expression. The string returned by ErrIndexNotViable.Error includes reasons why the index is
// considered non-viable.
type ErrIndexNotViable struct {
	IndexName        string   `json:"indexName"`
	NotViableReasons []string `json:"notViableReasons,omitempty"`
}

func (e ErrIndexNotViable) Error() string {
	bytes, _ := json.Marshal(e)
	reasonsJSON := string(bytes)
	return fmt.Sprintf("index not viable for expression: %s", reasonsJSON)
}

// ErrItemNotFound is returned by Get when an item with the provided key is not found in the table.
type ErrItemNotFound struct{}

func (ErrItemNotFound) Error() string {
	return "item not found"
}
