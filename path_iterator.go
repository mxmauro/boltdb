package boltdb

// -----------------------------------------------------------------------------

type pathIterator struct {
	path    []byte
	pathLen int
	offset  int
}

// -----------------------------------------------------------------------------

func newPathIterator(path []byte) (pathIterator, error) {
	pi := pathIterator{
		path:    path,
		pathLen: len(path),
	}

	// Skip leading slashes.
	for pi.offset < pi.pathLen {
		if pi.path[pi.offset] != '/' {
			break
		}
		pi.offset += 1
	}
	if pi.offset >= pi.pathLen {
		return pathIterator{}, ErrInvalidPath
	}

	// Done
	return pi, nil
}

func (pi *pathIterator) fragment() ([]byte, bool) {
	// Get next fragment.
	fragmentStart := pi.offset
	for pi.offset < pi.pathLen {
		if pi.path[pi.offset] == '/' {
			break
		}
		pi.offset += 1
	}
	fragmentEnd := pi.offset

	// Skip slashes.
	for pi.offset < pi.pathLen {
		if pi.path[pi.offset] != '/' {
			break
		}
		pi.offset += 1
	}

	// If we already were at the end...
	if fragmentStart == fragmentEnd {
		return nil, true
	}

	// Done
	return pi.path[fragmentStart:fragmentEnd], pi.offset >= pi.pathLen
}
