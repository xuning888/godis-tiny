package protocol

var (
	NullBulkReplyBytes = []byte("$-1" + CRCF)
	PongReplyBytes     = []byte("+PONG" + CRCF)
)

type PongReply struct {
}

func (p *PongReply) ToBytes() []byte {
	return PongReplyBytes
}
