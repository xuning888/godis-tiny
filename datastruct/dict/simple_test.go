package dict

import (
	"log"
	"testing"
)

func TestGet(t *testing.T) {
	dict := MakeSimpleDict()
	val, exists := dict.Get("aaa")
	if exists {
		log.Println(val)
	}
}
