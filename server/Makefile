BINARY_NAME=bit-box
.DEFAULT_GOAL=build

build:
	GOARCH=amd64 GOOS=linux go build -ldflags "-s -w" -o ./build/${BINARY_NAME}-linux main.go
	GOARCH=amd64 GOOS=darwin go build -ldflags "-s -w"  -o ./build/${BINARY_NAME}-darwin main.go
	GOARCH=amd64 GOOS=windows go build -ldflags "-s -w" -o ./build/${BINARY_NAME}-windows main.go

clean:
	go clean
	rm -fr ./build

