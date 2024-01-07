package protocol

var (
	NullBulkReplyBytes  = []byte("$-1" + CRLF)
	emptyMultiBulkBytes = []byte("*0" + CRLF)
	PongReplyBytes      = []byte("+PONG" + CRLF)
	okReplyBytes        = []byte("+OK" + CRLF)
	synTaxReplyBytes    = []byte("-ERR syntax error" + CRLF)
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

type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

type OkReply struct{}

func (o *OkReply) ToBytes() []byte {
	return okReplyBytes
}

func MakeOkReply() *OkReply {
	return &OkReply{}
}

type SyntaxReply struct {
}

func MakeSyntaxReply() *SyntaxReply {
	return &SyntaxReply{}
}

func (s *SyntaxReply) ToBytes() []byte {
	return synTaxReplyBytes
}
