package models

// BatchRequest is the HTTP request body for POST /v1/batches.
type BatchRequest struct {
	ClientID        string   `json:"client_id"`
	Reset           bool     `json:"reset"`
	CurrentPosition string   `json:"current_position,omitempty"`
	NextPosition    string   `json:"next_position"`
	Records         []Record `json:"records"`
}

// BatchResponse is the HTTP response for batch submission.
type BatchResponse struct {
	Status           string `json:"status"`
	NextPosition     string `json:"next_position,omitempty"`
	ExpectedPosition string `json:"expected_position,omitempty"`
	Message          string `json:"message,omitempty"`
}

// PositionResponse is the HTTP response for position lookup.
type PositionResponse struct {
	Status          string `json:"status"`
	CurrentPosition string `json:"current_position,omitempty"`
	Message         string `json:"message,omitempty"`
}
