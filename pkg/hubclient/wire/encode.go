package wire

import "encoding/binary"

// MakeHeader builds a wire header with host-order field values.
func MakeHeader(typ, nid, flag, payloadLen uint32) Header {
	return Header{
		Type:   typ,
		NID:    nid,
		Flag:   flag,
		Length: payloadLen,
		Chksum: Checksum,
	}
}

// EncodeHeader writes h into out (must be at least Size bytes).
func EncodeHeader(h Header, out []byte) {
	binary.BigEndian.PutUint32(out[0:4], h.Type)
	binary.BigEndian.PutUint32(out[4:8], h.NID)
	binary.BigEndian.PutUint32(out[8:12], h.Flag)
	binary.BigEndian.PutUint32(out[12:16], h.Length)
	binary.BigEndian.PutUint32(out[16:20], h.Chksum)
}

// EncodeFrame returns [Header | payload].
func EncodeFrame(typ, nid, flag uint32, payload []byte) []byte {
	h := MakeHeader(typ, nid, flag, uint32(len(payload)))
	frame := make([]byte, Size+len(payload))
	EncodeHeader(h, frame)
	copy(frame[Size:], payload)
	return frame
}
