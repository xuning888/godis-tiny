package redis

const (
	CRLF               = "\r\n"
	PING               = "+PONG" + CRLF
	NullBulk           = "$-1" + CRLF
	EmptyMultiBulk     = "*0" + CRLF
	OKReply            = "+OK" + CRLF
	SyntaxReplyS       = "-ERR syntax error" + CRLF
	OutOfRangeOrNotInt = "-ERR value is not an integer or out of range" + CRLF
	wrongTypeStr       = "-WRONGTYPE Operation against a key holding the wrong kind of value" + CRLF
)

var (
	CRLFBytes               = []byte(CRLF)
	pongReplyBytes          = []byte(PING)
	nullBulkReplyBytes      = []byte(NullBulk)
	emptyMultiBulkBytes     = []byte(EmptyMultiBulk)
	okReplyBytes            = []byte(OKReply)
	synTaxReplyBytes        = []byte(SyntaxReplyS)
	outOfRangeOrNotIntBytes = []byte(OutOfRangeOrNotInt)
	wrongTypeErrBytes       = []byte(wrongTypeStr)
)

var (
	poneReply             = &PongReply{}
	nullBulkReply         = &NullBulkReply{}
	emptyMultiBulkReply   = &EmptyMultiBulkReply{}
	wrongTypeErrReply     = &WrongTypeErrReply{}
	okReply               = &OkReply{}
	syntaxReply           = &SyntaxReply{}
	outOfRangeOrNotIntErr = &OutOfRangeOrNotIntErr{}
)

type PongReply struct{}

func (*PongReply) WriteTo(client *Client) error {
	_, err := client.Write(pongReplyBytes)
	if err != nil {
		return err
	}
	return client.Flush()
}

func (*PongReply) ToBytes() []byte {
	return pongReplyBytes
}

func MakePongReply() *PongReply {
	return poneReply
}

type NullBulkReply struct{}

func (n *NullBulkReply) WriteTo(client *Client) error {
	if _, err := client.Write(nullBulkReplyBytes); err != nil {
		return err
	}
	return client.Flush()
}

func (n *NullBulkReply) ToBytes() []byte {
	return nullBulkReplyBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return nullBulkReply
}

type EmptyMultiBulkReply struct{}

func (e *EmptyMultiBulkReply) WriteTo(client *Client) error {
	if _, err := client.Write(emptyMultiBulkBytes); err != nil {
		return err
	}
	return client.Flush()
}

func (e *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return emptyMultiBulkReply
}

type WrongTypeErrReply struct{}

func (w *WrongTypeErrReply) WriteTo(client *Client) error {
	if _, err := client.Write(wrongTypeErrBytes); err != nil {
		return err
	}
	return client.Flush()
}

// ToBytes marshals redis.Reply
func (w *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (w *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

func MakeWrongTypeErrReply() *WrongTypeErrReply {
	return wrongTypeErrReply
}

type OkReply struct{}

func (o *OkReply) WriteTo(client *Client) error {
	if _, err := client.Write(okReplyBytes); err != nil {
		return err
	}
	return client.Flush()
}

func (o *OkReply) ToBytes() []byte {
	return okReplyBytes
}

func MakeOkReply() *OkReply {
	return okReply
}

type SyntaxReply struct {
}

func (s *SyntaxReply) WriteTo(client *Client) error {
	if _, err := client.Write(synTaxReplyBytes); err != nil {
		return err
	}
	return client.Flush()
}

func (s *SyntaxReply) ToBytes() []byte {
	return synTaxReplyBytes
}

func MakeSyntaxReply() *SyntaxReply {
	return syntaxReply
}

type OutOfRangeOrNotIntErr struct{}

func (o *OutOfRangeOrNotIntErr) WriteTo(client *Client) error {
	if _, err := client.Write(outOfRangeOrNotIntBytes); err != nil {
		return err
	}
	return client.Flush()
}

func (o *OutOfRangeOrNotIntErr) ToBytes() []byte {
	return outOfRangeOrNotIntBytes
}

func MakeOutOfRangeOrNotInt() *OutOfRangeOrNotIntErr {
	return outOfRangeOrNotIntErr
}
