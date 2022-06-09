# Top K Heavy Hitters

Implementation of Top K heavy hitters using map reduce algorithm on distributed systems in Golang.

## Installation

 - [Golang](https://nodejs.org/).
 - [Protocol buffer](https://grpc.io/docs/protoc-installation/)
 - go plugins
    ```sh
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
    
    export PATH="$PATH:$(go env GOPATH)/bin"
    ```

### Configurations
    > K - First K items
    > Workers - IP address of worker nodes separated by ";"
    > InputPath - Path for Input files
    > OutputPath - Path to store intermediate output files after Map

#### Building for source

Compile proto 
```sh
protoc --go_out=. --go-grpc_out=. mrService.proto
```

Generating worker and master binaries

```sh
go build -o ./bin/ ./worker
go build -o ./bin/ ./master
```

Run 

```sh
go run worker/worker.go 
go run main.go --config ../config.ini
```
