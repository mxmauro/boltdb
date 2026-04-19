package boltdb

// -----------------------------------------------------------------------------

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copiedValue := make([]byte, len(value))
	copy(copiedValue, value)
	return copiedValue
}
