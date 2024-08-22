package util

import "errors"

var ErrAlreadySync = errors.New("already sync")
var ErrFileIsTooRecent = errors.New("file is too recent")
var ErrTimeframeTooSmall = errors.New("timeframe is too small")
var ErrSetNotFound = errors.New("set not found")
var ErrAssetNotFound = errors.New("asset not found")
var ErrAlreadyExists = errors.New("already exists")
var ErrInvalidDataKeyFormat = errors.New("invalid data key format")
