package log

// Having to add this because I'm having trouble importing "github.com/travisjeffery/proglog/api/v1"

type Record struct {
	Value  []byte `json:"value,omitempty"`
	Offset uint64 `json:"offset,omitempty"`
}
