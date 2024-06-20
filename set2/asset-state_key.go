package set2

import (
	"errors"
	"fmt"
	"strings"

	pcommon "github.com/pendulea/pendule-common"
)

type ColumnType byte

const READ_LIST_COLUMN ColumnType = 0
const LAST_INDEXATION_TIME_COLUMN ColumnType = 1
const DATA_COLUMN ColumnType = 255

func (sk *AssetState) GetReadListKey() []byte {
	prefix := append(sk.key[:], byte(READ_LIST_COLUMN))
	suffix := "read_list"
	return append(prefix, []byte(suffix)...)
}

func (sk *AssetState) GetDataKey(timeFrameLabel string, time pcommon.TimeUnit) []byte {
	prefix := append(sk.key[:], byte(DATA_COLUMN))
	suffix := fmt.Sprintf("%s:%d", timeFrameLabel, time)
	return append(prefix, []byte(suffix)...)
}

func (sk *AssetState) GetLastDataTimeKey(timeFrameLabel string) []byte {
	prefix := append(sk.key[:], byte(LAST_INDEXATION_TIME_COLUMN))
	return append(prefix, []byte(timeFrameLabel)...)
}

func (sk *AssetState) ParseDataKey(key []byte) (timeFrameLabel string, time pcommon.TimeUnit, err error) {
	//remove first 2 bytes
	keyFormated := key[2:]
	if len(keyFormated) > 0 && keyFormated[0] == byte(DATA_COLUMN) {
		keyStr := string(keyFormated[1:])
		var unixTime int64
		parts := strings.Split(keyStr, ":")
		if len(parts) == 2 {
			fmt.Sscanf(parts[1], "%d", &unixTime)
			if unixTime > 0 {
				return parts[0], pcommon.NewTimeUnit(unixTime), nil
			}
		}
	}
	return "", 0, errors.New("invalid tick key format")
}
