package redis

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrorCommandNotFund = errors.New("command not found")
	commandRouter       = make(map[string]*Command)
)

type Process func(ctx context.Context, conn *Client) error

type Command struct {
	name    string
	process Process
}

func register(name string, process Process) {
	cmd := &Command{
		name:    strings.ToLower(name),
		process: process,
	}
	commandRouter[name] = cmd
}

func router(name string) (*Command, error) {
	lowerName := strings.ToLower(name)
	if cmd, ok := commandRouter[lowerName]; ok {
		return cmd, nil
	}
	return nil, ErrorCommandNotFund
}
