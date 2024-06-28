package util

import "errors"

var ErrAlreadySync = errors.New("already sync")
var ErrFileIsTooRecent = errors.New("file is too recent")
var ErrTimeframeTooSmall = errors.New("timeframe is too small")
