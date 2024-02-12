package database

import (
	"fmt"
	"log"
	"strings"
)

var cmdTable = make(map[string]*command)

type command struct {
	cmdName string
	exeFunc ExeFunc
}

func RegisterCmd(cmdName string, exeFunc ExeFunc) {
	lower := strings.ToLower(cmdName)
	cmd := &command{
		cmdName: lower,
		exeFunc: exeFunc,
	}
	log.Println(fmt.Sprintf("register command %s", cmd.cmdName))
	cmdTable[lower] = cmd
}

func getCommand(cmdName string) *command {
	cmd, ok := cmdTable[cmdName]
	if ok {
		return cmd
	}
	return nil
}
