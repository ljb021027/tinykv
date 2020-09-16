package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type standaloneReader struct {
	txn *badger.Txn
}

func NewStandaloneReader(txn *badger.Txn) *standaloneReader {
	return &standaloneReader{
		txn: txn,
	}
}

func (s *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *standaloneReader) Close() {
	s.txn.Discard()
}
