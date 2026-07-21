//go:build !darwin && !linux

package packevidencegate

import (
	"errors"
	"net"
)

func peerUID(net.Conn) (uint32, error) {
	return 0, errors.New("pack evidence gate: peer credentials are unsupported on this platform")
}
