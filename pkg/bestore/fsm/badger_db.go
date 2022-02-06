package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/echoface/be_indexer/util"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

// badgerFSM raft.FSM implementation using badgerDB
type badgerFSM struct {
	db *badger.DB
}

// get fetch data from badgerDB
func (b badgerFSM) get(key string) (interface{}, error) {
	var keyByte = []byte(key)
	var data interface{}

	txn := b.db.NewTransaction(false)
	defer func() {
		_ = txn.Commit()
	}()

	item, err := txn.Get(keyByte)
	if err != nil {
		data = map[string]interface{}{}
		return data, err
	}

	var value = make([]byte, 0)
	err = item.Value(func(val []byte) error {
		value = append(value, val...)
		return nil
	})

	if err != nil {
		data = map[string]interface{}{}
		return data, err
	}

	if value != nil && len(value) > 0 {
		err = json.Unmarshal(value, &data)
	}

	if err != nil {
		data = map[string]interface{}{}
	}

	return data, err
}

// set store data to badgerDB
func (b badgerFSM) set(key string, value interface{}) error {
	var data = make([]byte, 0)
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if data == nil || len(data) <= 0 {
		return nil
	}

	txn := b.db.NewTransaction(true)
	err = txn.Set([]byte(key), data)
	if err != nil {
		txn.Discard()
		return err
	}

	return txn.Commit()
}

// delete remove data from badgerDB
func (b badgerFSM) delete(key string) error {
	var keyByte = []byte(key)

	txn := b.db.NewTransaction(true)
	err := txn.Delete(keyByte)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (b badgerFSM) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		var payload = CommandPayload{}
		if err := json.Unmarshal(log.Data, &payload); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error marshalling store payload %s\n", err.Error())
			return nil
		}

		fmt.Printf("raft fsm cmd:%s\n", util.JSONPretty(payload))

		op := strings.ToUpper(strings.TrimSpace(payload.Operation))
		switch op {
		case "SET":
			return &ApplyResponse{
				Error: b.set(payload.Key, payload.Value),
				Data:  payload.Value,
			}
		case "GET":
			data, err := b.get(payload.Key)
			return &ApplyResponse{
				Error: err,
				Data:  data,
			}

		case "DELETE":
			return &ApplyResponse{
				Error: b.delete(payload.Key),
				Data:  nil,
			}
		}
	}

	_, _ = fmt.Fprintf(os.Stderr, "not raft log command type\n")
	return nil
}

// Snapshot will be called during make snapshot.
// Snapshot is used to support log compaction.
// No need to call snapshot since it already persisted in disk (using BadgerDB) when raft calling Apply function.
func (b badgerFSM) Snapshot() (raft.FSMSnapshot, error) {
	fmt.Printf("raft fsm snapshot called:\n")
	return newFSMSnapshot(b.db)
}

// Restore is used to restore an FSM from a Snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
// Restore will update all data in BadgerDB
func (b badgerFSM) Restore(rClose io.ReadCloser) (err error) {
	_, _ = fmt.Fprintf(os.Stdout, "[RESTORE] enter\n")

	defer func() {
		_, _ = fmt.Fprintf(os.Stdout, "[RESTORE] leave with:%v:\n", err)
		if ie := rClose.Close(); ie != nil {
			_, _ = fmt.Fprintf(os.Stdout, "[RESTORE] close err:%s\n", ie.Error())
			err = fmt.Errorf("restore err:%v, snapshot close err:%s", err, ie.Error())
		}
	}()
	return b.db.Load(rClose, 1024)
}

// NewBadger raft.FSM implementation using badgerDB
func NewBadger(badgerDB *badger.DB) raft.FSM {
	return &badgerFSM{
		db: badgerDB,
	}
}
