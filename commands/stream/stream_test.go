package stream

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestValidateStartAtResume(t *testing.T) {
	startAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	err := validateStartAtResume(startAt, "token")
	if err == nil {
		t.Fatal("expected error when both start-at and resume-token are set")
	}
	if err.Error() != "Cannot use both start-at and resume-token" {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := validateStartAtResume(startAt, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateStartAtResume(time.Time{}, "token"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateStartAtResume(time.Time{}, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePipeline(t *testing.T) {
	pipeline, err := parsePipeline("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipeline) != 0 {
		t.Fatalf("expected empty pipeline, got %d stages", len(pipeline))
	}

	pipeline, err = parsePipeline(`{"$match":{"operationType":"insert"}}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pipeline) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(pipeline))
	}

	expected := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}},
	}
	if !reflect.DeepEqual(pipeline, expected) {
		t.Fatalf("unexpected pipeline: %#v", pipeline)
	}

	if _, err := parsePipeline(`{"$match":`); err == nil {
		t.Fatal("expected error for invalid pipeline JSON")
	}
}

func TestFormatTableRow(t *testing.T) {
	longValue := strings.Repeat("a", 150)
	raw := mustRaw(t, bson.D{{Key: "msg", Value: longValue}})
	event := StreamEvent{
		ID:            "id123",
		WallTime:      time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
		OperationType: "insert",
		FullDocument:  raw,
		DocumentKey:   bson.M{"_id": "abc"},
	}

	row := formatTableRow(event, true, false)
	if len(row) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(row))
	}
	if row[0] != event.ID {
		t.Fatalf("expected event ID at column 0, got %#v", row[0])
	}
	if row[1] != event.WallTime.String() {
		t.Fatalf("expected wallTime at column 1, got %#v", row[1])
	}

	expectedDoc := raw.String()
	if len(expectedDoc) > 100 {
		expectedDoc = expectedDoc[:100] + "..."
	}
	if row[4] != expectedDoc {
		t.Fatalf("unexpected document string: %#v", row[4])
	}

	row = formatTableRow(event, false, true)
	if len(row) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(row))
	}
	if row[3] != raw.String() {
		t.Fatalf("expected full document without truncation")
	}
}

func TestStreamEventToTable_NoFullDocument(t *testing.T) {
	raw := mustRaw(t, bson.D{{Key: "a", Value: "b"}})
	event := StreamEvent{
		WallTime:      time.Date(2025, 1, 3, 4, 5, 6, 0, time.UTC),
		OperationType: "update",
		FullDocument:  raw,
		Ns: struct {
			Db   string `bson:"db"`
			Coll string `bson:"coll"`
		}{Db: "db1", Coll: "coll1"},
	}

	expected := fmt.Sprintf("%s %s %s %s ", event.WallTime.String(), event.OperationType, event.Ns.Db, event.Ns.Coll)
	if got := event.ToTable(false); got != expected {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestStreamEventToTable_WithFullDocument(t *testing.T) {
	shortRaw := mustRaw(t, bson.D{{Key: "a", Value: "b"}})
	shortEvent := StreamEvent{
		WallTime:      time.Date(2025, 1, 3, 4, 5, 6, 0, time.UTC),
		OperationType: "insert",
		FullDocument:  shortRaw,
		Ns: struct {
			Db   string `bson:"db"`
			Coll string `bson:"coll"`
		}{Db: "db2", Coll: "coll2"},
	}
	shortPrefix := fmt.Sprintf("%s %s %s %s ", shortEvent.WallTime.String(), shortEvent.OperationType, shortEvent.Ns.Db, shortEvent.Ns.Coll)
	shortExpected := shortPrefix + fmt.Sprintf("%s\n", shortRaw)
	if got := shortEvent.ToTable(true); got != shortExpected {
		t.Fatalf("unexpected output for short document: %q", got)
	}

	longValue := strings.Repeat("z", 150)
	longRaw := mustRaw(t, bson.D{{Key: "msg", Value: longValue}})
	longEvent := StreamEvent{
		WallTime:      time.Date(2025, 1, 4, 5, 6, 7, 0, time.UTC),
		OperationType: "replace",
		FullDocument:  longRaw,
		Ns: struct {
			Db   string `bson:"db"`
			Coll string `bson:"coll"`
		}{Db: "db3", Coll: "coll3"},
	}
	longPrefix := fmt.Sprintf("%s %s %s %s ", longEvent.WallTime.String(), longEvent.OperationType, longEvent.Ns.Db, longEvent.Ns.Coll)
	docString := longRaw.String()
	if len(docString) <= 100 {
		t.Fatalf("expected long document string, got length %d", len(docString))
	}
	longExpected := longPrefix + docString[:100] + "...}\n"
	if got := longEvent.ToTable(true); got != longExpected {
		t.Fatalf("unexpected output for long document: %q", got)
	}
}

func mustRaw(t *testing.T, doc any) bson.Raw {
	t.Helper()
	data, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("failed to marshal bson: %v", err)
	}
	return bson.Raw(data)
}
