package sds

type Sds []byte

func (s *Sds) Len() int {
	return len(*s)
}

func (s *Sds) Remining() int {
	return cap(*s) - len(*s)
}

func (s *Sds) Memory() int {
	return cap(*s)
}

func (s *Sds) SdsCat(b []byte) {
	if len(b) > s.Remining() {
		bytes := make([]byte, s.Len()+len(b))
		copy(bytes, *s)
		copy(bytes[s.Len():], b)
		*s = bytes
	} else {
		*s = append(*s, b...)
	}
}

func (s *Sds) Free() {
	*s = nil
}

func (s *Sds) String() string {
	return string(*s)
}

func New(s string) *Sds {
	sdss := Sds(s)
	return &sdss
}

func NewEmpty() *Sds {
	sdss := make(Sds, 0)
	return &sdss
}

func NewWithBytes(b []byte) *Sds {
	sdss := Sds(b)
	return &sdss
}
