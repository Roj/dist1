package storage

const (
	Read = iota
	Write
	Newserver
	Finishserver
)
type Query struct {
	Type int
	Hostname string
	Node Node
}
type ResultsResponse struct {
	Finished bool
	Node Node
}