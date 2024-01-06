package main

import (
	"g-redis/redis/server"
	"g-redis/tcp"
)

func main() {
	err := tcp.ListenAndServeWithSignal(":8080", server.MakeHandler())
	if err != nil {
		panic(err)
	}
}
