package conf

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	confFile string
)

func init() {
	flag.StringVar(&confFile, "confile", "", "config path")
}

func getPlatform(cf string) (string, error) {
	_, fileName := filepath.Split(cf)
	fileNameWithoutExt := strings.Split(fileName, ".")
	if len(fileNameWithoutExt) != 2 {
		return "", fmt.Errorf("config file should have file extension type (ex: yml/yaml, json)")
	}
	pf := strings.Split(fileNameWithoutExt[0], "-")
	if len(pf) != 2 {
		return "", fmt.Errorf("config file doesn't have platform details (ex: dev/stg/prod/<any value for testing>)")
	}

	return pf[1], nil
}

func ValidateConfigFile(testConf bool) error {
	pf, err := getPlatform(confFile)
	if err != nil {
		return err
	}
	scopeEnv := os.Getenv("SCOPE_ENV")

	if scopeEnv == "" {
		if !testConf {
			return fmt.Errorf("can't run this service for local testing")
		}

		if pf == "dev" || pf == "stg" || pf == "prod" {
			return fmt.Errorf("Use different config file (other than config-dev.yml/config-stg.yml/config-prod.yml), while running service for local testing")
		}
	}
	return nil
}

func Load(conf interface{}) error {

	if reflect.TypeOf(conf).Kind() != reflect.Ptr || reflect.TypeOf(conf).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("conf should be of type pointer to struct")
	}
	if confFile == "" {
		return fmt.Errorf("config path is not set, 'confFile' flag should be set to config file")
	}

	log.Infof("Service using conf file: %s", confFile)
	viper.SetConfigFile(confFile)

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("error reading config file, %v", err)
	}

	if err := viper.Unmarshal(conf); err != nil {
		return fmt.Errorf("failed to unmarshal config values, %v", err)
	}
	return nil
}

func ValidateConf(conf interface{}) error {
	var v reflect.Value
	if reflect.TypeOf(conf).Kind() == reflect.Struct {
		v = reflect.ValueOf(conf)
	} else if reflect.TypeOf(conf).Kind() == reflect.Ptr && reflect.TypeOf(conf).Elem().Kind() == reflect.Struct {
		v = reflect.ValueOf(conf).Elem()
	} else {
		return fmt.Errorf("conf should be struct or pointer to struct")
	}
	if keys := getKeysNotSet(v); len(keys) > 0 {
		return fmt.Errorf("values not configured for key(s): %v", strings.Join(keys, ", "))
	}
	return nil
}

func getAllKeys(v reflect.Value) []string {
	var keys []string

	if v.Kind() != reflect.Struct {
		return keys
	}

	for i := 0; i < v.NumField(); i++ {
		val := v.Field(i)
		f := v.Type().Field(i)
		var fieldName string
		if f.Tag.Get("mapstructure") != "" {
			tagVals := strings.Split(f.Tag.Get("mapstructure"), ",")
			if len(tagVals) > 1 && tagVals[1] == "squash" {
				continue
			}
			fieldName = tagVals[0]
		} else {
			fieldName = f.Name
		}
		if val.Kind() == reflect.Struct {
			if params := getAllKeys(val); len(params) > 0 {
				for _, p := range params {
					if !f.Anonymous {
						keys = append(keys, (fieldName + "." + p))
					} else {
						keys = append(keys, p)
					}
				}
			}
		} else if val.Kind() == reflect.Map {
			for _, mapKey := range val.MapKeys() {
				if params := getAllKeys(val.MapIndex(mapKey)); len(params) > 0 {
					for _, p := range params {
						keys = append(keys, (fieldName + "." + mapKey.String() + "." + p))
					}
				}
			}
		} else {
			keys = append(keys, fieldName)
		}
	}
	return keys
}

func getKeysNotSet(v reflect.Value) []string {
	keys := getAllKeys(v)
	if len(keys) == 0 {
		return nil
	}

	keysNotSet := make([]string, 0, len(keys))
	for _, k := range keys {
		if !viper.IsSet(k) {
			keysNotSet = append(keysNotSet, k)
		}
	}
	return keysNotSet
}

func SetConfFile(cfn string) {
	confFile = cfn
}
