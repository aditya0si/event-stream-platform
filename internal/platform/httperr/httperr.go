// Package httperr writes the JSON error envelope every endpoint uses.
//
// One shape for every failure, because a client should not have to learn a different error
// format per endpoint, and because the alternative — bare status codes with prose — cannot be
// handled programmatically. The envelope carries a stable machine-readable code, a message
// meant for a developer, and optionally the field at fault so a client can attach the error
// to the input it came from.
package httperr

import (
	"encoding/json"
	"net/http"

	"github.com/aditya0si/event-stream-platform/internal/platform/reqid"
)

// Envelope is the wire shape of every error this API returns.
type Envelope struct {
	Error Error `json:"error"`
	// RequestID is echoed so that a client reporting a problem can quote one string that
	// finds the corresponding server-side log line.
	RequestID string `json:"request_id,omitempty"`
}

// Error is the body of Envelope.
type Error struct {
	// Code is stable and machine-readable. Clients branch on this; they must not branch on
	// Message, which is for humans and may be reworded.
	Code string `json:"code"`
	// Message is a single sentence naming what was wrong. It never contains a stack trace,
	// an internal identifier, or a value the caller did not send.
	Message string `json:"message"`
	// Details carries structured extras — the offending field, the count that was exceeded.
	Details map[string]any `json:"details,omitempty"`
}

// Codes used by this service. Kept in one place so a client's switch statement and the
// server's call sites cannot drift apart.
const (
	CodeValidationFailed  = "validation_failed"
	CodeUnauthorized      = "unauthorized"
	CodePayloadTooLarge   = "payload_too_large"
	CodeBrokerUnavailable = "broker_unavailable"
	CodeInternal          = "internal_error"
	CodeNotFound          = "not_found"
	CodeMethodNotAllowed  = "method_not_allowed"
)

// Write sends an error envelope and records nothing else. Callers are responsible for
// incrementing their own outcome counters, because only the caller knows which metric
// describes the failure.
func Write(w http.ResponseWriter, r *http.Request, status int, code, message string, details map[string]any) {
	body := Envelope{
		Error: Error{Code: code, Message: message, Details: details},
	}
	if r != nil {
		body.RequestID = reqid.FromContext(r.Context())
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Errors are never cached: a 503 that gets cached turns a transient outage into a
	// permanent one for that client.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// Invalid reports a validation failure, naming the field when one is known.
func Invalid(w http.ResponseWriter, r *http.Request, field, message string) {
	var details map[string]any
	if field != "" {
		details = map[string]any{"field": field}
	}
	Write(w, r, http.StatusBadRequest, CodeValidationFailed, message, details)
}

// Unauthorized reports a missing or wrong credential. The message is deliberately the same
// for both cases: distinguishing them tells an attacker which half they got right.
func Unauthorized(w http.ResponseWriter, r *http.Request) {
	Write(w, r, http.StatusUnauthorized, CodeUnauthorized,
		"a valid API key is required: send it as X-API-Key or as an Authorization: Bearer token", nil)
}

// TooLarge reports a body beyond the configured limit.
func TooLarge(w http.ResponseWriter, r *http.Request, limit int64) {
	Write(w, r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
		"the request body exceeds the configured limit", map[string]any{"limit_bytes": limit})
}

// Unavailable reports a dependency this request needed, with a retry hint. The Retry-After
// header is what distinguishes "come back shortly" from "this request is wrong", and a client
// that honours it will not hammer a struggling dependency.
func Unavailable(w http.ResponseWriter, r *http.Request, message string) {
	w.Header().Set("Retry-After", "1")
	Write(w, r, http.StatusServiceUnavailable, CodeBrokerUnavailable, message, nil)
}

// Internal reports an unexpected failure. The message is fixed and generic on purpose: the
// underlying error goes to the log with the request id, not to the client, because internals
// leak in error strings more often than anywhere else.
func Internal(w http.ResponseWriter, r *http.Request) {
	Write(w, r, http.StatusInternalServerError, CodeInternal,
		"an unexpected internal error occurred; quote the request_id when reporting it", nil)
}
