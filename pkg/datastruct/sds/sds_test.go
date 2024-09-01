package sds

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSds(t *testing.T) {
	testCases := []struct {
		name     string
		initial  Sds
		toAppend []byte
		expected Sds
	}{
		{
			name:     "Append to Empty Sds",
			initial:  Sds(""),
			toAppend: []byte("Hello"),
			expected: Sds("Hello"),
		},
		{
			name:     "Append to Non-Empty Sds With Enough Capacity",
			initial:  append(make(Sds, 5, 10), 'H', 'e', 'l', 'l', 'o'),
			toAppend: []byte(", World!"),
			expected: Sds("Hello, World!"),
		},
		{
			name:     "Append to Non-Empty Sds Without Enough Capacity",
			initial:  Sds("Hi"),
			toAppend: []byte(", there! How are you?"),
			expected: Sds("Hi, there! How are you?"),
		},
		{
			name:     "Append with Empty Input",
			initial:  Sds("Hello"),
			toAppend: []byte(""),
			expected: Sds("Hello"),
		},
		{
			name:     "Consecutive Append with Capacity Expanding",
			initial:  Sds("Init"),
			toAppend: []byte("__additional_text__"),
			expected: Sds("Init__additional_text__"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			initialCopy := append([]byte{}, tt.initial...) // Preserve initial state
			tt.initial.SdsCat(tt.toAppend)
			if !bytes.Equal(tt.initial, tt.expected) {
				fmt.Printf("test %s failed: expected %q, got %q\n", tt.name, tt.expected, tt.initial)
			}
			tt.initial = initialCopy
		})
	}
}
