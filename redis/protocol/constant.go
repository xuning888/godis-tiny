package protocol

var (
	NullBulkReplyBytes      = []byte("$-1" + CRLF)
	emptyMultiBulkBytes     = []byte("*0" + CRLF)
	PongReplyBytes          = []byte("+PONG" + CRLF)
	okReplyBytes            = []byte("+OK" + CRLF)
	synTaxReplyBytes        = []byte("-ERR syntax error" + CRLF)
	outOfRangeOrNotIntBytes = []byte("-ERR value is not an integer or out of range" + CRLF)
)

var (
	// pongReply
	pongReply = &PongReply{}
	// nullBulkReply
	nullBulkReply = &NullBulkReply{}
	// emptyMultiReply
	emptyMultiReply = &EmptyMultiBulkReply{}
	// ok
	okReply = &OkReply{}
	// syntaxReply
	syntaxReply = &SyntaxReply{}
	// outOfRangeOrNotIntErr
	outOfRangeOrNotIntErr = &OutOfRangeOrNotIntErr{}
	// wrongTypeErrReply
	wrongTypeErrReply = &WrongTypeErrReply{}
)

type PongReply struct {
}

func (p *PongReply) ToBytes() []byte {
	return PongReplyBytes
}

func MakePongReply() *PongReply {
	return pongReply
}

type NullBulkReply struct {
}

func (n *NullBulkReply) ToBytes() []byte {
	return NullBulkReplyBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return nullBulkReply
}

type EmptyMultiBulkReply struct{}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return emptyMultiReply
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

func MakeWrongTypeErrReply() ErrReply {
	return wrongTypeErrReply
}

type OkReply struct{}

func (o *OkReply) ToBytes() []byte {
	return okReplyBytes
}

func MakeOkReply() *OkReply {
	return okReply
}

type SyntaxReply struct {
}

func MakeSyntaxReply() *SyntaxReply {
	return syntaxReply
}

func (s *SyntaxReply) ToBytes() []byte {
	return synTaxReplyBytes
}

// OutOfRangeOrNotIntErr ERR value is not an integer or out of range
type OutOfRangeOrNotIntErr struct {
}

func (o *OutOfRangeOrNotIntErr) ToBytes() []byte {
	return outOfRangeOrNotIntBytes
}

func MakeOutOfRangeOrNotInt() *OutOfRangeOrNotIntErr {
	return outOfRangeOrNotIntErr
}
