package set2

import (
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

type destructor struct {
	db          *badger.DB
	currentTX   *badger.Txn
	rMKey       chan string
	err         error
	queued      atomic.Int32
	preCommited atomic.Int32
	once        sync.Once
}

// Batch size for deletion
const BATCH_SIZE = 10_000

// You need to close the destructor when you're done with it
func newDestructor(db *badger.DB) *destructor {
	d := &destructor{
		db:          db,
		currentTX:   db.NewTransaction(true),
		rMKey:       make(chan string, 10000), // Buffer to avoid blocking
		err:         nil,
		queued:      atomic.Int32{},
		preCommited: atomic.Int32{},
	}

	go d.process()
	return d
}

func (d *destructor) delete(key []byte) {
	if d.err == nil {
		d.queued.Add(1)
		d.rMKey <- string(key)
	}
}

func (d *destructor) process() {
	for key := range d.rMKey {
		// Delete the key from the transaction
		d.err = d.currentTX.Delete([]byte(key))
		if d.err != nil {
			d.close()
			return
		}

		n := d.preCommited.Add(1)
		d.queued.Add(-1)

		// Commit and reset after 10,000 keys
		if n >= BATCH_SIZE {
			if d.err = d.currentTX.Commit(); d.err != nil {
				d.close()
				return
			}

			// Create a new transaction
			d.currentTX = d.db.NewTransaction(true)
			d.preCommited.Store(0)
		}
	}

	// Commit remaining transaction if any
	if d.preCommited.Load() > 0 {
		if d.err = d.currentTX.Commit(); d.err != nil {
			d.close()
			return
		}
		d.preCommited.Store(0)
	}
}

func (d *destructor) forceClose() {
	d.preCommited.Store(0)
	d.queued.Store(0)
	d.close()
}

func (d *destructor) close() {
	d.once.Do(func() { // Ensure the channel is closed only once
		if d.err == nil {
			for (d.queued.Load() > 0 && d.preCommited.Load() > 0) || d.err != nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
		// Close the RMKey channel to stop receiving new keys
		close(d.rMKey)

		// Discard the current transaction if it's still open
		if d.currentTX != nil {
			d.currentTX.Discard()
		}
	})
}
