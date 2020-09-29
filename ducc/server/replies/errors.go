package replies

type DUCCReply interface {
	IsOk() bool
	IsErr() bool
	Tag() string
}

type DUCCError struct {
	Tag string `json:"tag"`
	Err string `json:"error"`
}

func NewDUCCError(tag string, err error) DUCCError {
	return DUCCError{tag, err.Error()}
}

func (e DUCCError) IsOk() bool    { return false }
func (e DUCCError) IsErr() bool   { return true }
func (e DUCCError) Error() string { return e.Err }

type DUCCStatError struct {
	DUCCError
	Path string `json:"path"`
}

func NewDUCCStatError(path string, err error) DUCCStatError {
	generic := NewDUCCError("error-stat", err)
	return DUCCStatError{generic, path}
}

type LayerStatusOk struct {
	Status string `json:"status"`
}

func NewLayerStatusOk() LayerStatusOk {
	return LayerStatusOk{"ok"}
}

type LayerStatusErr struct {
	Status     string          `json:"status"`
	StatErrors []DUCCStatError `json:"stat_errors"`
}

func NewLayerStatusErr(errors []DUCCStatError) LayerStatusErr {
	return LayerStatusErr{"err", errors}
}

type LayerSuccessfullyIngested struct {
	Digest string `json:"digest"`
}

func NewLayerSuccessfullyIngested(digest string) LayerSuccessfullyIngested {
	return LayerSuccessfullyIngested{digest}
}
