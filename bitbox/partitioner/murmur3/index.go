package murmur3

func GetHash128(s string) (uint64, uint64) {
	h := New128WithSeed(1000)
	h.Write([]byte(s))
	return h.Sum128()
}

func GetHash64(s string) uint64 {
	h := New64WithSeed(1000)
	h.Write([]byte(s))
	return h.Sum64()
}
