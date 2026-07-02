package wire

import (
	"encoding/binary"
	"errors"
)

var (
	ErrShortHeader  = errors.New("wire: header too short")
	ErrBadChecksum  = errors.New("wire: invalid checksum")
	ErrShortPayload = errors.New("wire: payload too short")
)

// DecodeHeader parses in into out. Returns ErrBadChecksum when chksum mismatches.
func DecodeHeader(in []byte, out *Header) error {
	if len(in) < Size {
		return ErrShortHeader
	}
	out.Type = binary.BigEndian.Uint32(in[0:4])
	out.NID = binary.BigEndian.Uint32(in[4:8])
	out.Flag = binary.BigEndian.Uint32(in[8:12])
	out.Length = binary.BigEndian.Uint32(in[12:16])
	out.Chksum = binary.BigEndian.Uint32(in[16:20])
	if out.Chksum != Checksum {
		return ErrBadChecksum
	}
	return nil
}

// ValidateChecksum reports whether h has the expected magic chksum.
func ValidateChecksum(h Header) bool {
	return h.Chksum == Checksum
}
