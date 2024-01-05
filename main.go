package main

import "g-redis/tcp"

func main() {
	err := tcp.ListenAndServeWithSignal(":8080", &tcp.EchoHandler{})
	if err != nil {
		panic(err)
	}
}
