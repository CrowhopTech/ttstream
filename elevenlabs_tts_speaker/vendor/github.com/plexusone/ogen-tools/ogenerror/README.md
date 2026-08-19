# ogenerror

Extract error details from ogen-generated client errors.

## Problem

When an ogen-generated client receives an unexpected HTTP status code, it returns an error like:

```
decode response: unexpected status code: 403
```

The response body (which often contains useful error details) is buried in the error and not easily accessible.

## Solution

This package provides utilities to extract the status code and response body from ogen errors.

## Installation

```bash
go get github.com/agentplexus/ogen-tools/ogenerror
```

## Usage

### Parse full error details

```go
import "github.com/agentplexus/ogen-tools/ogenerror"

resp, err := client.SomeMethod(ctx, req)
if err != nil {
    if status := ogenerror.Parse(err); status != nil {
        fmt.Printf("Status: %d\n", status.StatusCode)
        fmt.Printf("Body: %s\n", status.Body)
    }
}
```

### Check status code

```go
if ogenerror.IsStatus(err, 403) {
    // Handle forbidden
}

if ogenerror.Is4xx(err) {
    // Handle client error
}

if ogenerror.Is5xx(err) {
    // Handle server error
}
```

### Get just the status code

```go
code := ogenerror.StatusCode(err)
if code == 429 {
    // Rate limited
}
```

## API

| Function | Description |
|----------|-------------|
| `Parse(err) *UnexpectedStatus` | Extract status code and body |
| `StatusCode(err) int` | Get just the status code (0 if not ogen error) |
| `IsStatus(err, code) bool` | Check for specific status code |
| `Is4xx(err) bool` | Check if 4xx client error |
| `Is5xx(err) bool` | Check if 5xx server error |

## Example: API-specific error parsing

```go
// In your API client package
func ParseAPIError(err error) *MyAPIError {
    status := ogenerror.Parse(err)
    if status == nil {
        return nil
    }

    apiErr := &MyAPIError{StatusCode: status.StatusCode}

    // Parse your API's specific error format
    var resp struct {
        Error struct {
            Message string `json:"message"`
            Code    string `json:"code"`
        } `json:"error"`
    }
    if json.Unmarshal(status.Body, &resp) == nil {
        apiErr.Message = resp.Error.Message
        apiErr.Code = resp.Error.Code
    }

    return apiErr
}
```
