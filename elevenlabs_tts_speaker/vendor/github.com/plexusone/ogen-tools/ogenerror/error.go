// Package ogenerror provides utilities for extracting error details from
// ogen-generated client errors.
package ogenerror

import (
	"errors"
	"io"

	"github.com/ogen-go/ogen/validate"
)

// UnexpectedStatus contains the status code and response body from an
// ogen UnexpectedStatusCodeError.
type UnexpectedStatus struct {
	StatusCode int
	Body       []byte
}

// Parse extracts status code and response body from an ogen error.
// Returns nil if the error is not an ogen UnexpectedStatusCodeError.
//
// Usage:
//
//	resp, err := client.SomeMethod(ctx, req)
//	if err != nil {
//	    if status := ogenerror.Parse(err); status != nil {
//	        fmt.Printf("Status: %d, Body: %s\n", status.StatusCode, status.Body)
//	    }
//	}
func Parse(err error) *UnexpectedStatus {
	if err == nil {
		return nil
	}

	var ogenErr *validate.UnexpectedStatusCodeError
	if !errors.As(err, &ogenErr) {
		return nil
	}

	result := &UnexpectedStatus{
		StatusCode: ogenErr.StatusCode,
	}

	// Try to read the response body
	if ogenErr.Payload != nil && ogenErr.Payload.Body != nil {
		body, readErr := io.ReadAll(ogenErr.Payload.Body)
		if readErr == nil {
			result.Body = body
		}
	}

	return result
}

// StatusCode extracts just the status code from an ogen error.
// Returns 0 if the error is not an ogen UnexpectedStatusCodeError.
func StatusCode(err error) int {
	if status := Parse(err); status != nil {
		return status.StatusCode
	}
	return 0
}

// IsStatus returns true if the error is an ogen UnexpectedStatusCodeError
// with the given status code.
func IsStatus(err error, code int) bool {
	return StatusCode(err) == code
}

// Is4xx returns true if the error is a 4xx client error.
func Is4xx(err error) bool {
	code := StatusCode(err)
	return code >= 400 && code < 500
}

// Is5xx returns true if the error is a 5xx server error.
func Is5xx(err error) bool {
	code := StatusCode(err)
	return code >= 500 && code < 600
}
