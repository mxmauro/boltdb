package boltdb

// -----------------------------------------------------------------------------

func removeLeadingSlashes(path []byte) []byte {
	var ofs int
	for ofs = 0; ofs < len(path) && path[ofs] == '/'; ofs++ {
	}
	return path[ofs:]
}

func removeTrailingSlashes(path []byte) []byte {
	var ofs int
	for ofs = len(path); ofs > 0 && path[ofs-1] == '/'; ofs-- {
	}
	return path[:ofs]
}

func getPathFragmentLen(path []byte) int {
	var ofs int
	for ofs = 0; ofs < len(path) && path[ofs] != '/'; ofs++ {
	}
	return ofs
}
