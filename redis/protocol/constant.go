package protocol

var (
	NullBulkReply = []byte("$-1" + CRCF)
)
