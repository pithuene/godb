package main

type Database struct {
	Pager *Pager
}

func OpenDatabase(filename string) (*Database, error) {
	pager, err := OpenPager(filename)
	if err != nil {
		return nil, err
	}
	return &Database{
		Pager: pager,
	}, nil
}

func (db *Database) Close() {
	db.Pager.Close()
}

type EntryHeader struct {
	InUse bool
}

const EntryHeaderMaskInUse = 0b00000001

func (eh *EntryHeader) Serialize() byte {
	var res byte
	if eh.InUse {
		res |= EntryHeaderMaskInUse
	}
	return res
}

func DeserializeEntryHeader(serialized byte) *EntryHeader {
	var eh EntryHeader
	if serialized&EntryHeaderMaskInUse != 0 {
		eh.InUse = true
	}
	return &eh
}
