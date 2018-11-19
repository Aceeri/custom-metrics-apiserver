package provider

import (
	"context"
	"fmt"
	"strconv"

	"github.com/golang/glog"
)

// ListenForStatsd listens to UDP packets for service queue depth.
func (statsd *StatsdProvider) ListenForStatsd(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			break
		default:
		}

		buf := make([]byte, 1024)
		n, err := statsd.listener.Read(buf)
		if n > 0 || err != nil {
			glog.Infof("Received: `%s` (n:%d, e:%s)", buf[0:n], n, err)
			parsed, err := ParsePacket(buf[0:n])
			if err != nil {
				glog.Errorf("Could not parse packet (%s): %s", buf[0:n], err)
				continue
			}

			glog.Infof("Parsed: %+v", parsed)
		}
	}
}

type ServiceDepth struct {
	Name    string
	Sector  string
	Ident   string
	Address string
	Depth   int
}

// Parse a subset of statsd packets that is in the format of:
// `queue_depth.<service name>.<sector>.<service ident>.<service address>:<queue depth value>|g`
func ParsePacket(packet []byte) (*ServiceDepth, error) {
	var depth ServiceDepth

	parser := PacketParser{
		buffer: packet,
		index:  0,
	}

	word, err := parser.TakeUntilDelimiter(".")
	if err != nil {
		return nil, err
	}

	if word != "queue_depth" {
		return nil, fmt.Errorf("Not a queue depth packet")
	}

	depth.Name, err = parser.TakeUntilDelimiter(".")
	if err != nil {
		return nil, fmt.Errorf("Error while taking name: %s", err)
	}

	depth.Sector, err = parser.TakeUntilDelimiter(".")
	if err != nil {
		return nil, fmt.Errorf("Error while taking sector: %s", err)
	}

	depth.Ident, err = parser.TakeUntilDelimiter(".")
	if err != nil {
		return nil, fmt.Errorf("Error while taking ident: %s", err)
	}

	depth.Address, err = parser.TakeUntilDelimiter(":")
	if err != nil {
		return nil, fmt.Errorf("Error while taking address: %s", err)
	}

	queueDepth, err := parser.TakeUntilDelimiter("|")
	if err != nil {
		return nil, fmt.Errorf("Error while taking queue depth: %s", err)
	}

	depth.Depth, err = strconv.Atoi(queueDepth)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert depth (%s) to integer: %s", queueDepth, err)
	}

	err = parser.Verify("g")
	if err != nil {
		return nil, fmt.Errorf("Could not fully verify packet: %s", err)
	}

	return &depth, nil
}

type PacketParser struct {
	buffer []byte
	index  int
}

// Takes characters until it hits the specified delimiter or one of the other possible delimiters (`.`, `:`, `|`).
func (parser *PacketParser) TakeUntilDelimiter(delimiter string) (string, error) {
	word := ""
Top:
	for {
		if parser.index >= len(parser.buffer) {
			return "", fmt.Errorf("Buffer ended unexpectedly")
		}

		c := string(parser.buffer[parser.index])
		parser.index++

		switch c {
		case ".", ":", "|":
			if c == delimiter {
				break Top
			} else {
				return "", fmt.Errorf("Unexpected delimiter %s, expected %s", c, delimiter)
			}
		default:
			word += c
		}
	}

	return word, nil
}

// Verify that the next character is the delimiter.
func (parser *PacketParser) Verify(delimiter string) error {
	if parser.index >= len(parser.buffer) {
		return fmt.Errorf("Buffer ended unexpectedly")
	}

	c := string(parser.buffer[parser.index])
	if c != delimiter {
		return fmt.Errorf("Unexpected character %s, expected %s", c, delimiter)
	}

	return nil
}
