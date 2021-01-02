# Notes

On master run:
`go run mrmaster.go pg-*.txt`

On workers run:
`go build -buildmode=plugin ../mrapps/wc.go; go run mrworker.go wc.so`