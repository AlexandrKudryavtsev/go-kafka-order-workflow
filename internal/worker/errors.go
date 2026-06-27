package worker

type ErrorKind string

const (
	RetryableError    ErrorKind = "retryable"
	NonRetryableError ErrorKind = "non_retryable"
)

type ProcessingError struct {
	Kind   ErrorKind
	Reason string
	Err    error
}

func (e *ProcessingError) Error() string {
	if e.Err == nil {
		return e.Reason
	}

	return e.Reason + ": " + e.Err.Error()
}

func (e *ProcessingError) Unwrap() error {
	return e.Err
}

func Retryable(reason string, err error) error {
	return &ProcessingError{
		Reason: reason,
		Err:    err,
		Kind:   RetryableError,
	}
}

func NonRetryable(reason string, err error) error {
	return &ProcessingError{
		Reason: reason,
		Err:    err,
		Kind:   NonRetryableError,
	}
}
