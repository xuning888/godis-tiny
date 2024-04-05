.PHONY: packet

BINARY_NAME=godis-tiny
GOBUILD=go build

packet:
	mkdir -p godis-tiny-server/bin
	mkdir -p godis-tiny-server/conf
	go mod tidy
	go build -o $(BINARY_NAME) main.go
	mv $(BINARY_NAME) godis-tiny-server/bin
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) -o $(BINARY_NAME)-windows-amd64.exe main.go
	mv $(BINARY_NAME)-windows-amd64.exe godis-tiny-server/bin
	CGO_ENABLED=0 GOOS=windows GOARCH=386 $(GOBUILD) -o $(BINARY_NAME)-windows-386.exe main.go
	mv $(BINARY_NAME)-windows-386.exe godis-tiny-server/bin
	cp ./redis.conf godis-tiny-server/conf
	zip -r godis-tiny-server.zip ./godis-tiny-server
	rm -rf godis-tiny-server/