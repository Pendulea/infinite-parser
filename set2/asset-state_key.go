package set2

import (
	"errors"
	"pendulev2/util"

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
	suffix := append([]byte(timeFrameLabel), util.Int64ToBytes(time.Int())...)

	return append(prefix, suffix...)
}

func (sk *AssetState) GetLastDataTimeKey(timeFrameLabel string) []byte {
	prefix := append(sk.key[:], byte(LAST_INDEXATION_TIME_COLUMN))
	return append(prefix, []byte(timeFrameLabel)...)
}

func (sk *AssetState) ParseDataKey(key []byte) (timeFrameLabel string, time pcommon.TimeUnit, err error) {
	//remove first 2 bytes
	keyFormated := key[2:]
	if len(keyFormated) > 0 && keyFormated[0] == byte(DATA_COLUMN) {
		keyFormated = keyFormated[1:]
		last8Bytes := keyFormated[len(keyFormated)-8:]
		if len(last8Bytes) != 8 {
			return "", 0, errors.New("invalid tick key format")
		}
		keyFormated = keyFormated[:len(keyFormated)-8]
		if len(keyFormated) < 2 {
			return "", 0, errors.New("invalid tick key format")
		}
		return string(keyFormated), pcommon.NewTimeUnit(util.BytesToInt64(last8Bytes)), nil
	}
	return "", 0, errors.New("invalid tick key format")
}
