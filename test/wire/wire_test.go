package wire_test

import (
	"bytes"
	"testing"

	"github.com/sunchao1/hi-im-hubclient/pkg/hubclient/wire"
)

func TestHeaderSize(t *testing.T) {
	if wire.Size != 20 {
		t.Fatalf("Size = %d, want 20", wire.Size)
	}
}

func TestFieldOrderMatchesPackedLayout(t *testing.T) {
	h := wire.MakeHeader(0x11111111, 0x22222222, 0x33333333, 0x44444444)
	out := make([]byte, wire.Size)
	wire.EncodeHeader(h, out)

	want := []byte{
		0x11, 0x11, 0x11, 0x11,
		0x22, 0x22, 0x22, 0x22,
		0x33, 0x33, 0x33, 0x33,
		0x44, 0x44, 0x44, 0x44,
		0x1F, 0xE2, 0x3D, 0xC4,
	}
	if !bytes.Equal(out, want) {
		t.Fatalf("header bytes mismatch:\n got % x\nwant % x", out, want)
	}
}

func TestEndianRoundTrip(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	frame := wire.EncodeFrame(0x030B, 20001, wire.FlagExp, payload)

	var hdr wire.Header
	if err := wire.DecodeHeader(frame, &hdr); err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if hdr.Type != 0x030B || hdr.NID != 20001 || hdr.Flag != wire.FlagExp ||
		hdr.Length != 4 || hdr.Chksum != wire.Checksum {
		t.Fatalf("unexpected header: %+v", hdr)
	}
	if !bytes.Equal(frame[wire.Size:], payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestRejectsInvalidChecksum(t *testing.T) {
	h := wire.MakeHeader(1, 2, 0, 0)
	h.Chksum = 0xDEADBEEF
	out := make([]byte, wire.Size)
	wire.EncodeHeader(h, out)

	var decoded wire.Header
	if err := wire.DecodeHeader(out, &decoded); err != wire.ErrBadChecksum {
		t.Fatalf("want ErrBadChecksum, got %v", err)
	}
}

func TestAuthBodyLayout(t *testing.T) {
	body := wire.EncodeAuthBody(10, "proxy", "secret", 40001)
	if len(body) != wire.AuthBodyLen {
		t.Fatalf("auth body len = %d, want %d", len(body), wire.AuthBodyLen)
	}
}

func TestSnapPartialAndSticky(t *testing.T) {
	f1 := wire.EncodeFrame(wire.CmdKpaliveReq, 1, wire.FlagSys, nil)
	f2 := wire.EncodeFrame(wire.CmdKpaliveAck, 1, wire.FlagSys, nil)
	data := append(append([]byte{}, f1[:10]...), f1[10:]...)
	data = append(data, f2...)

	snap := &wire.Snap{}
	snap.Append(data[:15])
	if fr, err := snap.PopFrame(); fr != nil || err != nil {
		t.Fatalf("expected need-more-data, got frame=%v err=%v", fr, err)
	}
	snap.Append(data[15:])
	fr1, err := snap.PopFrame()
	if err != nil || fr1 == nil || fr1.Header.Type != wire.CmdKpaliveReq {
		t.Fatalf("first frame: fr=%v err=%v", fr1, err)
	}
	fr2, err := snap.PopFrame()
	if err != nil || fr2 == nil || fr2.Header.Type != wire.CmdKpaliveAck {
		t.Fatalf("second frame: fr=%v err=%v", fr2, err)
	}
}

func TestSnapInvalidChecksumClearsBuffer(t *testing.T) {
	bad := wire.MakeHeader(1, 2, wire.FlagSys, 0)
	bad.Chksum = 0xBAD
	buf := make([]byte, wire.Size)
	wire.EncodeHeader(bad, buf)

	snap := &wire.Snap{}
	snap.Append(buf)
	if _, err := snap.PopFrame(); err != wire.ErrBadChecksum {
		t.Fatalf("want ErrBadChecksum, got %v", err)
	}
	if snap.Buffered() != 0 {
		t.Fatalf("buffer should be cleared")
	}
}
