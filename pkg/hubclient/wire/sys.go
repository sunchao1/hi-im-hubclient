package wire

import (
	"encoding/binary"
)

const (
	CmdAuthReq    = 0x0001
	CmdAuthAck    = 0x0002
	CmdKpaliveReq = 0x0003
	CmdKpaliveAck = 0x0004
	CmdSubReq     = 0x0005
	CmdSubAck     = 0x0006

	AuthUserLen = 32
	AuthPassLen = 16
	AuthBodyLen = 4 + AuthUserLen + AuthPassLen + 4
)

// EncodeAuthBody builds AUTH_REQ payload aligned with hi-im-core auth.hpp.
func EncodeAuthBody(gid uint32, user, pass string, nid uint32) []byte {
	body := make([]byte, AuthBodyLen)
	binary.BigEndian.PutUint32(body[0:4], gid)
	copy(body[4:4+AuthUserLen], user)
	copy(body[4+AuthUserLen:4+AuthUserLen+AuthPassLen], pass)
	binary.BigEndian.PutUint32(body[4+AuthUserLen+AuthPassLen:], nid)
	return body
}

// DecodeAuthAck returns true when AUTH succeeded.
// hi-im-core sends an empty payload on success; beehive may send is_succ=1 (4 bytes).
func DecodeAuthAck(payload []byte) (ok bool, err error) {
	if len(payload) == 0 {
		return true, nil
	}
	if len(payload) < 4 {
		return false, ErrShortPayload
	}
	return binary.BigEndian.Uint32(payload[:4]) == 1, nil
}

// EncodeSubBody builds SUB_REQ payload (uint32 BE sub_cmd).
func EncodeSubBody(subCmd uint32) []byte {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, subCmd)
	return body
}
