package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"math"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, math.MaxUint64))

	return &Scanner{
		txn:  txn,
		iter: iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	next := func(scan *Scanner) {
		item := scan.iter.Item()
		for ; scan.iter.Valid(); scan.iter.Next() {
			if !bytes.Equal(DecodeUserKey(item.Key()), DecodeUserKey(scan.iter.Item().Key())) {
				break
			}
		}
	}

	for scan.iter.Valid() {
		key := DecodeUserKey(scan.iter.Item().Key())
		value, err := scan.txn.GetValue(key)
		if err != nil {
			return nil, nil, err
		}
		next(scan)
		if value != nil {
			return key, value, nil
		}
	}

	return nil, nil, nil
}
