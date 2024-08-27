package util

import (
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

type Destructor struct {
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
func NewDestructor(db *badger.DB) *Destructor {
	d := &Destructor{
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

func (d *Destructor) Error() error {
	return d.err
}

func (d *Destructor) Delete(key []byte) {
	if d.err == nil {
		d.queued.Add(1)
		d.rMKey <- string(key)
	}
}

func (d *Destructor) process() {
	for key := range d.rMKey {
		// Delete the key from the transaction
		d.err = d.currentTX.Delete([]byte(key))
		if d.err != nil {
			return
		}

		n := d.preCommited.Add(1)
		// Commit and reset after 10,000 keys
		if n >= BATCH_SIZE {
			if d.err = d.currentTX.Commit(); d.err != nil {
				d.queued.Add(-1)
				return
			}

			// Create a new transaction
			d.currentTX = d.db.NewTransaction(true)
			d.preCommited.Store(0)
		}

		d.queued.Add(-1)
	}
}

func (d *Destructor) Discard() {
	d.once.Do(func() { // Ensure the channel is closed only once
		if d.err == nil {
			for d.queued.Load() > 0 {
				time.Sleep(10 * time.Millisecond)
				if d.err != nil {
					break
				}
			}
		}

		// Close the RMKey channel to stop receiving new keys
		close(d.rMKey)

		if d.err == nil {
			// Commit remaining transaction if any
			if d.preCommited.Load() > 0 {
				d.err = d.currentTX.Commit()
				d.preCommited.Store(0)
			}
		}

		// Discard the current transaction if it's still open
		if d.currentTX != nil {
			d.currentTX.Discard()
		}
	})
}
