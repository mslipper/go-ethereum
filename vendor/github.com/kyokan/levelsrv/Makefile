deps:
	echo "--> Installing deps..."
	@dep ensure -v

protogen:
	protoc -I pkg/proto pkg/proto/node.proto --go_out=plugins=grpc:pkg/pb

build: protogen
	go build -o ./build/levelsrv ./cmd/levelsrv/main.go

.PHONY: build