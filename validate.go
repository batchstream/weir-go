package weir

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/protobuf/proto"
)

var storeName = regexp.MustCompile(`^[a-z](?:[a-z0-9]|-[a-z0-9]){0,62}$`)
var mediaType = regexp.MustCompile(`^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$`)

// Resource constructs a canonical Weir URI from decoded resource segments.
// Examples: Resource("mongo", "database", "records", "s:record-id") and
// Resource("search", "index", "s:record-id"). Omitting segments gives a Store URI.
func Resource(store string, segments ...string) (string, error) {
	parts := []string{"weir://" + store}
	for _, segment := range segments {
		parts = append(parts, encodeSegment(segment))
	}
	uri := strings.Join(parts, "/")
	if _, _, err := parseResource(uri); err != nil {
		return "", err
	}
	return uri, nil
}

func encodeSegment(segment string) string {
	var out strings.Builder
	for _, b := range []byte(segment) {
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-._~:", rune(b)) {
			out.WriteByte(b)
		} else {
			fmt.Fprintf(&out, "%%%02X", b)
		}
	}
	return out.String()
}

func parseResource(resource string) (string, int, error) {
	if len(resource) > 4096 || !strings.HasPrefix(resource, "weir://") {
		return "", 0, fmt.Errorf("weir: invalid resource URI")
	}
	parts := strings.Split(strings.TrimPrefix(resource, "weir://"), "/")
	if len(parts[0]) > 63 || !storeName.MatchString(parts[0]) {
		return "", 0, fmt.Errorf("weir: invalid Store name")
	}
	for _, part := range parts[1:] {
		decoded, err := url.PathUnescape(part)
		if err != nil || decoded == "" || decoded == "." || decoded == ".." || !utf8.ValidString(decoded) || encodeSegment(decoded) != part {
			return "", 0, fmt.Errorf("weir: invalid or noncanonical resource segment")
		}
		if strings.ContainsFunc(decoded, unicode.IsControl) {
			return "", 0, fmt.Errorf("weir: control character in resource")
		}
	}
	return parts[0], len(parts) - 1, nil
}

func validateMedia(media string) bool { return len(media) <= 127 && mediaType.MatchString(media) }
func validateDocument(doc *pb.Document) error {
	if doc == nil || !validateMedia(doc.MediaType) || len(doc.Data) > MaxDocumentBytes {
		return fmt.Errorf("weir: missing or invalid document envelope (maximum 256 KiB)")
	}
	return nil
}
func validateRecord(resource string) error {
	_, count, err := parseResource(resource)
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("weir: record resource needs path segments")
	}
	return nil
}

func validateRead(req *pb.ReadRequest) error {
	if req == nil || proto.Size(req) > MaxFrameBytes {
		return fmt.Errorf("weir: missing or oversized Read")
	}
	if err := validateRecord(req.Resource); err != nil {
		return err
	}
	if req.ReadMediaType != "" && !validateMedia(req.ReadMediaType) {
		return fmt.Errorf("weir: invalid Read media type")
	}
	if req.AdapterOptions != nil {
		return validateDocument(req.AdapterOptions)
	}
	return nil
}

func validateMutation(req *pb.MutateRequest) error {
	if req == nil || proto.Size(req) > MaxFrameBytes {
		return fmt.Errorf("weir: missing or oversized mutation")
	}
	if err := validateRecord(req.Resource); err != nil {
		return err
	}
	if req.AdapterOptions != nil {
		if err := validateDocument(req.AdapterOptions); err != nil {
			return err
		}
	}
	switch action := req.Action.(type) {
	case *pb.MutateRequest_Put:
		return validateDocument(action.Put)
	case *pb.MutateRequest_Create:
		return validateDocument(action.Create)
	case *pb.MutateRequest_Replace:
		return validateDocument(action.Replace)
	case *pb.MutateRequest_Delete:
		if action.Delete != nil {
			return nil
		}
	case *pb.MutateRequest_AtomicTransform:
		expression := action.AtomicTransform.GetBackendExpression()
		if expression == nil || len(expression.Data) == 0 || len(expression.Data) > 16<<10 {
			return fmt.Errorf("weir: only bounded backend-expression transforms are supported")
		}
		return validateDocument(expression)
	}
	return fmt.Errorf("weir: missing or unsupported mutation action")
}
