package wire

const (
	Size = 20

	Checksum = 0x1FE23DC4

	FlagSys = 0
	FlagExp = 1
)

// Header is the 20-byte bus wire v1 header (packed, big-endian on the wire).
type Header struct {
	Type   uint32
	NID    uint32
	Flag   uint32
	Length uint32
	Chksum uint32
}
