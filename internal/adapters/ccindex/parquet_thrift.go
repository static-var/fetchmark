package ccindex

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

const (
	maxParquetThriftDepth  = 64
	maxParquetThriftTokens = 500_000
)

type thriftScanFrame struct {
	kind      thrift.Type
	element   thrift.Type
	key       thrift.Type
	value     thrift.Type
	remaining int32
	mapValue  bool
}

type countingThriftInput struct {
	reader io.Reader
	bytes  int
}

func (reader *countingThriftInput) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	reader.bytes += count
	return count, err
}

// validateCompactThriftStructure walks a compact-Thrift struct iteratively.
// It rejects excessive depth and work before parquet-go's reflective decoder
// can recurse through unknown fields or nested collection elements.
func validateCompactThriftStructure(ctx context.Context, input io.Reader, payloadBudget int) (int, error) {
	if ctx == nil || input == nil || payloadBudget <= 0 {
		return 0, errors.New("Parquet Thrift structural scan requires context, reader, and payload budget")
	}
	counted := &countingThriftInput{reader: contextReader{ctx: ctx, reader: input}}
	protocol := new(thrift.CompactProtocol)
	bounded := newBoundedThriftReader(protocol.NewReader(counted), payloadBudget)
	tokenLimit := maxParquetThriftTokens
	if scaled := payloadBudget * 2; scaled < tokenLimit {
		tokenLimit = scaled
	}
	frames := []thriftScanFrame{{kind: thrift.STRUCT}}
	tokens := 0
	for len(frames) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if len(frames) > maxParquetThriftDepth {
			return 0, fmt.Errorf("Parquet Thrift nesting depth exceeds %d", maxParquetThriftDepth)
		}
		frame := &frames[len(frames)-1]
		switch frame.kind {
		case thrift.STRUCT:
			field, err := bounded.ReadField()
			if err != nil {
				return 0, err
			}
			if field.Type == thrift.STOP {
				frames = frames[:len(frames)-1]
				continue
			}
			if err := spendThriftToken(&tokens, tokenLimit); err != nil {
				return 0, err
			}
			if err := pushThriftValue(bounded, &frames, field.Type, true); err != nil {
				return 0, err
			}
		case thrift.LIST, thrift.SET:
			if frame.remaining == 0 {
				frames = frames[:len(frames)-1]
				continue
			}
			frame.remaining--
			if err := spendThriftToken(&tokens, tokenLimit); err != nil {
				return 0, err
			}
			if err := pushThriftValue(bounded, &frames, frame.element, false); err != nil {
				return 0, err
			}
		case thrift.MAP:
			if frame.remaining == 0 {
				frames = frames[:len(frames)-1]
				continue
			}
			if err := spendThriftToken(&tokens, tokenLimit); err != nil {
				return 0, err
			}
			valueType := frame.key
			if frame.mapValue {
				valueType = frame.value
				frame.mapValue = false
				frame.remaining--
			} else {
				frame.mapValue = true
			}
			if err := pushThriftValue(bounded, &frames, valueType, false); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unsupported Parquet Thrift scan frame type %d", frame.kind)
		}
	}
	return counted.bytes, nil
}

func spendThriftToken(tokens *int, limit int) error {
	if *tokens >= limit {
		return fmt.Errorf("Parquet Thrift structural token count exceeds %d", limit)
	}
	(*tokens)++
	return nil
}

func pushThriftValue(reader *boundedThriftReader, frames *[]thriftScanFrame, valueType thrift.Type, fieldValue bool) error {
	switch valueType {
	case thrift.TRUE, thrift.FALSE:
		if fieldValue {
			return nil
		}
		_, err := reader.ReadBool()
		return err
	case thrift.I8:
		_, err := reader.ReadInt8()
		return err
	case thrift.I16:
		_, err := reader.ReadInt16()
		return err
	case thrift.I32:
		_, err := reader.ReadInt32()
		return err
	case thrift.I64:
		_, err := reader.ReadInt64()
		return err
	case thrift.DOUBLE:
		_, err := reader.ReadFloat64()
		return err
	case thrift.BINARY:
		return reader.skipBinary()
	case thrift.LIST:
		list, err := reader.ReadList()
		if err != nil {
			return err
		}
		*frames = append(*frames, thriftScanFrame{kind: thrift.LIST, element: normalizeThriftBoolType(list.Type), remaining: list.Size})
		return nil
	case thrift.SET:
		set, err := reader.ReadSet()
		if err != nil {
			return err
		}
		*frames = append(*frames, thriftScanFrame{kind: thrift.SET, element: normalizeThriftBoolType(set.Type), remaining: set.Size})
		return nil
	case thrift.MAP:
		value, err := reader.ReadMap()
		if err != nil {
			return err
		}
		*frames = append(*frames, thriftScanFrame{kind: thrift.MAP, key: normalizeThriftBoolType(value.Key), value: normalizeThriftBoolType(value.Value), remaining: value.Size})
		return nil
	case thrift.STRUCT:
		*frames = append(*frames, thriftScanFrame{kind: thrift.STRUCT})
		return nil
	case thrift.UUID:
		if _, err := reader.ReadFloat64(); err != nil {
			return err
		}
		_, err := reader.ReadFloat64()
		return err
	default:
		return fmt.Errorf("unsupported Parquet Thrift value type %d", valueType)
	}
}

func normalizeThriftBoolType(valueType thrift.Type) thrift.Type {
	if valueType == thrift.TRUE {
		return thrift.FALSE
	}
	return valueType
}

func (reader *boundedThriftReader) skipBinary() error {
	length, err := reader.thriftReaderDelegate.ReadLength()
	if err != nil {
		return err
	}
	if length < 0 || length > reader.maxBlobBytes || reader.directPayloadBytes > reader.maxBlobBytes-length {
		return fmt.Errorf("Parquet Thrift byte value exceeds bounded payload limit %d", reader.maxBlobBytes)
	}
	if _, err := io.CopyN(io.Discard, reader.thriftReaderDelegate.Reader(), int64(length)); err != nil {
		return err
	}
	reader.directPayloadBytes += length
	return nil
}
