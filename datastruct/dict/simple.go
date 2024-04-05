package dict

type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimpleDict() *SimpleDict {
	m := make(map[string]interface{})
	return &SimpleDict{
		m: m,
	}
}

func (s *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, ok := s.m[key]
	return val, ok
}

func (s *SimpleDict) Len() int {
	return len(s.m)
}

func (s *SimpleDict) Put(key string, val interface{}) (result int) {
	_, ok := s.m[key]
	s.m[key] = val
	if ok {
		return 0
	}
	return 1
}

func (s *SimpleDict) PutIfAbsent(key string, value interface{}) (result int) {
	_, ok := s.m[key]
	if ok {
		return 0
	}
	s.m[key] = value
	return 1
}

func (s *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	_, ok := s.m[key]
	if ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (s *SimpleDict) Remove(key string) (result int) {
	_, ok := s.m[key]
	delete(s.m, key)
	if ok {
		return 1
	}
	return 0
}

func (s *SimpleDict) ForEach(consumer Consumer) {
	for key, value := range s.m {
		if !consumer(key, value) {
			break
		}
	}
}

func (s *SimpleDict) Keys() []string {
	result := make([]string, s.Len())
	i := 0
	for k := range s.m {
		result[i] = k
		i++
	}
	return result
}

func (s *SimpleDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range s.m {
			result[i] = k
			break
		}
	}
	return result
}

func (s *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > s.Len() {
		size = s.Len()
	}
	result := make([]string, size)
	i := 0
	for k := range s.m {
		if i == size {
			break
		}
		result[i] = k
		i++
	}
	return result
}

func (s *SimpleDict) Clear() {
	s.m = make(map[string]interface{})
}
