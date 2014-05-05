package replay

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"os"
	"strings"
	"time"
)

// Reads the ops from a source and present a interface for consumers to fetch
// these ops sequentially.
type OpsReader interface {
	// Move to next op and return it. Nil will be returned if the last ops had
	// already been read, or there is any error occurred.
	// TODO change from Document to Op
	Next() *Op
	// How many ops are read so far
	OpsRead() int
	// Have all the ops been read?
	AllLoaded() bool
	// indicate the latest error occurs when reading ops.
	Err() error
	Close()
}

// ByLineOpsReader reads ops from a json file that is exported from python's
// json_util module, where each line is a json-represented op.
//
// Note: After parse each json-represented op, we need perform post-process to
// convert some "metadata" into MongoDB specific data structures, like "Object
// Id" and datetime.
type ByLineOpsReader struct {
	lineReader *bufio.Reader
	err        error
	opsRead    int
	closeFunc  func()
}

func NewByLineOpsReader(reader io.Reader) (error, *ByLineOpsReader) {
	return nil, &ByLineOpsReader{
		lineReader: bufio.NewReaderSize(reader, 5*1024*1024),
		err:        nil,
		opsRead:    0,
	}
}

// func NewCyclicOpsReader(func() ops_reader_maker *OpsReader) (error, OpsReader)

func NewFileByLineOpsReader(filename string) (error, *ByLineOpsReader) {
	file, err := os.Open(filename)
	if err != nil {
		return err, nil
	}
	err, reader := NewByLineOpsReader(file)
	if err != nil {
		return err, reader
	}
	reader.closeFunc = func() {
		file.Close()
	}
	return nil, reader
}
func (loader *ByLineOpsReader) Next() *Op {
	// we may need to skip certain type of ops
	for {
		jsonText, err := loader.lineReader.ReadString('\n')
		loader.err = err

		if err != nil && err != io.EOF {
			return nil
		}

		rawObj, err := parseJson(jsonText)
		loader.err = err
		if err != nil {
			return nil
		}
		loader.opsRead++
		op := makeOp(rawObj)
		if op == nil {
			continue
		}

		return op
	}
}

func (loader *ByLineOpsReader) OpsRead() int {
	return loader.opsRead
}

func (loader *ByLineOpsReader) AllLoaded() bool {
	return loader.err == io.EOF
}

func (loader *ByLineOpsReader) Err() error {
	return loader.err
}
func (loader *ByLineOpsReader) Close() {
	if loader.closeFunc != nil {
		loader.closeFunc()
	}
}

// Convert a json string to a raw document
func parseJson(jsonText string) (Document, error) {
	rawObj := Document{}
	err := json.Unmarshal([]byte(jsonText), &rawObj)

	if err != nil {
		return rawObj, err
	}
	normalizeObj(rawObj)
	return rawObj, err
}

// Detect if a document object is the "metadata". Right now we deal with two
// types of metadata:
// 1. document like { "$time": <timestamp> } will be converted to time.Time
// 	  object.
// 2. document like { "oid": <hex-string> } will be converted to ObjectId.
// @returns a boolean indicate if anything got converted; if yes, the second
// 			return value will be the converted object.
func parseMetadata(obj Document) (bool, interface{}) {
	// All the metadata are represented by a map object with single key/value
	// pair.
	if len(obj) != 1 {
		return false, nil
	}

	if obj["$date"] != nil {
		// all integers are parsed as float.
		date := (int64)(obj["$date"].(float64))
		// Represented as unix time, The last 3 digits are encodes "ms" while
		// the rest digits indicate "seconds".
		return true, time.Unix(
			date/1000, /* sec */
			date%1000*1000000 /* nano sec */)
	}

	if obj["$oid"] != nil {
		return true, bson.ObjectIdHex(obj["$oid"].(string))
	}

	return false, nil
}

// Recursively Replace some "metadata" from its string representation to the
// format that mgo recognizes.
func normalizeObj(rawObj Document) {
	for key, val := range rawObj {
		switch typedVal := val.(type) {
		default:
			continue
		case map[string]interface{}:
			updated, updatedObj := parseMetadata(typedVal)
			if !updated {
				// if not updated, recursively scanning the sub-doc for this
				// value.
				normalizeObj(typedVal)
			} else {
				rawObj[key] = updatedObj
			}
		case []interface{}:
			for i := range typedVal {
				switch item := typedVal[i].(type) {
				case map[string]interface{}:
					normalizeObj(item)
				default:
					continue
				}
			} // list iteration
		} // switch
	} // map iteration
}

func makeOp(rawDoc Document) *Op {
	opType := rawDoc["op"].(string)
	ts := rawDoc["ts"].(time.Time)
	ns := rawDoc["ns"].(string)
	parts := strings.SplitN(ns, ".", 2)
	dbName, collName := parts[0], parts[1]

	var content Document
	// we only handpick the fields that will be of useful for a given op type.
	switch opType {
	case "insert":
		content = Document{"o": rawDoc["o"].(map[string]interface{})}
	case "query":
		content = Document{
			"query":     rawDoc["query"],
			"ntoreturn": rawDoc["ntoreturn"],
			"ntoskip":   rawDoc["ntoskip"],
		}
	case "update":
		content = Document{
			"query":     rawDoc["query"],
			"updateobj": rawDoc["updateobj"],
		}
	case "command":
		content = Document{"command": rawDoc["command"]}
	case "remove":
		content = Document{"query": rawDoc["query"]}
	default:
		return nil
	}
	return &Op{dbName, collName, OpType(opType), ts, content}
}

type CyclicOpsReader struct {
	maker        func() OpsReader
	reader       OpsReader
	previousRead int
	err          error
}

func NewCyclicOpsReader(maker func() OpsReader) *CyclicOpsReader {
	reader := maker()
	if reader == nil {
		return nil
	}

	return &CyclicOpsReader{
		maker,
		reader,
		0,
		nil,
	}
}

func (self *CyclicOpsReader) Next() *Op {
	var op *Op = nil
	if op = self.reader.Next(); op == nil {
		log.Println("Recycle starts")
		self.previousRead += self.reader.OpsRead()
		self.reader.Close()
		self.reader = self.maker()
		op = self.reader.Next()
	}
	if op == nil {
		self.err = errors.New("The underlying ops reader is empty or invalid")
	}
	return op

}

func (self *CyclicOpsReader) OpsRead() int {
	return self.reader.OpsRead() + self.previousRead
}

func (self *CyclicOpsReader) AllLoaded() bool {
	return false
}

func (self *CyclicOpsReader) Err() error {
	if self.err != nil {
		return self.err
	}
	return self.reader.Err()
}

func (self *CyclicOpsReader) Close() {
	self.reader.Close()
}
