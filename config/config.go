package config

import (
	"bufio"
	"godis-tiny/pkg/util"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

var defaultMaxClients = 10000

type ServerProperties struct {
	RunID          string `cfg:"runid"`
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	Dir            string `cfg:"dir"`
	AppendOnly     bool   `cfg:"appendonly"`
	AppendFilename string `cfg:"appendfilename"`
	AppendFsync    string `cfg:"appendfsync"`
	MaxClients     int    `cfg:"maxclients"`

	// config file path
	CfPath string `cfg:"cf,omitempty"`
}

var Properties = &ServerProperties{}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}

func SetUpConfig(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer util.Close(file)
	Properties = parse(file)
	Properties.RunID = util.RandStr(40)
	configFilePath, err := filepath.Abs(filename)
	if err != nil {
		return
	}
	maxClinets := Properties.MaxClients
	if maxClinets == 0 {
		maxClinets = defaultMaxClients
	} else {
		if maxClinets > defaultMaxClients {
			maxClinets = defaultMaxClients
		}
	}
	Properties.MaxClients = maxClinets

	Properties.CfPath = configFilePath
	if Properties.Dir == "" {
		Properties.Dir = "."
	}
}
