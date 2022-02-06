package fsm

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

// fsmSnapshot handle noop snapshot
type fsmSnapshot struct {
	db *badger.DB
}

// Persist persist to disk. Return nil on success, otherwise return error.
func (s fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := s.db.Backup(sink, 0); err != nil {
		return err
	}
	return nil
}

// Release fsm can release the lock after persist snapshot.
// Release is invoked when we are finished with the snapshot.
func (s fsmSnapshot) Release() {
}

// newSnapshotNoop is returned by an FSM in response to a fsmSnapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
func newFSMSnapshot(db *badger.DB) (raft.FSMSnapshot, error) {
	return &fsmSnapshot{
		db: db,
	}, nil
}
