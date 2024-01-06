package protocol

var (
	NullBulkReplyBytes  = []byte("$-1" + CRLF)
	emptyMultiBulkBytes = []byte("*0" + CRLF)
	PongReplyBytes      = []byte("+PONG" + CRLF)
)

type PongReply struct {
}

func (p *PongReply) ToBytes() []byte {
	return PongReplyBytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

type NullBulkReply struct {
}

func (n *NullBulkReply) ToBytes() []byte {
	return NullBulkReplyBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

type EmptyMultiBulkReply struct{}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}
