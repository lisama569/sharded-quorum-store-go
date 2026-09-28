package store

import "fmt"

// ResultCode 是客户端操作可区分的结果类别。
type ResultCode int

const (
	ResultOK ResultCode = iota
	ResultTimeout
	ResultUnavailable
	ResultConflict
	ResultRejected
	ResultCorrupt
)

func (c ResultCode) String() string {
	switch c {
	case ResultOK:
		return "OK"
	case ResultTimeout:
		return "TIMEOUT"
	case ResultUnavailable:
		return "UNAVAILABLE"
	case ResultConflict:
		return "CONFLICT"
	case ResultRejected:
		return "REJECTED"
	case ResultCorrupt:
		return "CORRUPT"
	default:
		return "UNKNOWN"
	}
}

// OpError 携带可区分的失败码与诊断信息。
type OpError struct {
	Code   ResultCode
	Op     string
	Reason string
}

func (e *OpError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s failed: %s (%s)", e.Op, e.Code, e.Reason)
}

func opError(code ResultCode, op, reason string) *OpError {
	return &OpError{Code: code, Op: op, Reason: reason}
}

// CodeOf 从 error 中提取 ResultCode；nil 视为 OK。
func CodeOf(err error) ResultCode {
	if err == nil {
		return ResultOK
	}
	if e, ok := err.(*OpError); ok {
		return e.Code
	}
	return ResultRejected
}
