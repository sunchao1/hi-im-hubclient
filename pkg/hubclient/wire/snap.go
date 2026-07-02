package wire

// Frame is one complete wire frame after snap reassembly.
type Frame struct {
	Header  Header
	Payload []byte
}

// Snap reassembles TCP stream bytes into complete frames.
type Snap struct {
	buf []byte
}

// Append adds newly read TCP bytes to the internal buffer.
func (s *Snap) Append(data []byte) {
	if len(data) == 0 {
		return
	}
	s.buf = append(s.buf, data...)
}

// Buffered returns bytes waiting in the snap buffer.
func (s *Snap) Buffered() int {
	return len(s.buf)
}

// Clear drops all buffered bytes.
func (s *Snap) Clear() {
	s.buf = s.buf[:0]
}

// PopFrame extracts the next complete frame, or nil when more data is needed.
// Invalid checksum clears the buffer (aligned with hi-im-core FrameBuffer).
func (s *Snap) PopFrame() (*Frame, error) {
	if len(s.buf) < Size {
		return nil, nil
	}
	var hdr Header
	if err := DecodeHeader(s.buf, &hdr); err != nil {
		s.Clear()
		return nil, err
	}
	frameLen := Size + int(hdr.Length)
	if len(s.buf) < frameLen {
		return nil, nil
	}
	payload := make([]byte, hdr.Length)
	copy(payload, s.buf[Size:frameLen])
	s.buf = s.buf[frameLen:]
	return &Frame{Header: hdr, Payload: payload}, nil
}
