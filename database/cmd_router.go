package database

import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	cmdName string
	exeFunc ExeFunc
}

func RegisterCmd(cmdName string, exeFunc ExeFunc) {
	lower := strings.ToLower(cmdName)
	cmdTable[lower] = &command{
		cmdName: cmdName,
		exeFunc: exeFunc,
	}
}

func getCommand(cmdName string) *command {
	cmd, ok := cmdTable[cmdName]
	if ok {
		return cmd
	}
	return nil
}
