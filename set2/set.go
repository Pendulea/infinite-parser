package set2

import (
	"log"

	pcommon "github.com/pendulea/pendule-common"
	"github.com/sirupsen/logrus"

	badger "github.com/dgraph-io/badger/v4"
)

type Set struct {
	initialized bool
	Assets      map[string]*AssetState
	id          string
	dbPath      string
	db          *badger.DB
}

func (set *Set) Size() int64 {
	lsm, vlog := set.db.Size()
	return lsm + vlog
}

func (set *Set) RunValueLogGC() {
	for {
		err := set.db.RunValueLogGC(0.5)
		if err != nil {
			if err == badger.ErrNoRewrite {
				// No more space can be reclaimed
				break
			}
			log.Printf("Error running value log GC: %v", err)
			break
		}
	}
}

func (set *Set) ID() string {
	return set.id
}

func NewSet(id string, dbPath string) (*Set, error) {
	if err := pcommon.File.EnsureDir(pcommon.Env.DATABASES_DIR); err != nil {
		return nil, err
	}

	options := badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"symbol": id,
	}).Info("DB open")

	return &Set{
		db:          db,
		initialized: false,
		Assets:      make(map[string]*AssetState),
		id:          id,
		dbPath:      dbPath,
	}, nil
}

func (set *Set) Init() {
	if set.initialized {
		return
	}
	set.initialized = true
	if set.db == nil {
		log.Fatal("DB is nil")
	}
}

func (s *Set) Close() {
	if s.db != nil {
		logrus.WithFields(logrus.Fields{
			"symbol": s.ID(),
		}).Warn("Closing DB...")
		if err := s.db.Close(); err != nil {
			logrus.WithFields(logrus.Fields{
				"symbol": s.ID(),
				"msg":    err.Error(),
			}).Error("Error closing database connection")
		}
	}
}
