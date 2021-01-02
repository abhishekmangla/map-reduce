# Notes

On master run:
`go run mrmaster.go pg-*.txt`
go run src/main/mrmaster.go src/main/pg-*.txt

On workers run:
`go build -buildmode=plugin ../mrapps/wc.go; go run mrworker.go wc.so`
go build -o ./src/mrapps/wc.so -buildmode=plugin ./src/mrapps/wc.go; go run ./src/main/mrworker.go ./src/mrapps/wc.so

go build -o ./src/mrapps/crash.so -buildmode=plugin ./src/mrapps/crash.go; go run ./src/main/mrworker.go ./src/mrapps/crash.so