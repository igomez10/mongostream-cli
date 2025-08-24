# mongostream-cli

CLI for tailing MongoDB Change Streams for a specific collection. Supports JSON or table output, filtering via a pipeline, resume tokens, and starting from a specific timestamp.

- Entry point: [main.go](main.go)
- Stream command: [`stream.GetCmd`](commands/stream/stream.go)
- Event model: [`stream.StreamEvent`](commands/stream/stream.go), [`stream.EventID`](commands/stream/stream.go)

## Install

```bash
go install github.com/igomez10/mongostream-cli@latest
```

## Build from source

```bash
git clone https://github.com/igomez10/mongostream-cli.git
cd mongostream-cli
go build -o mongostream-cli
```

## Usage

```bash
mongostream-cli stream --database <db> --collection <coll> --url "mongodb://localhost:27017" [flags...]
```

Flags:
- --database string: Database name (required)
- --collection string: Collection name (required)
- --url string: MongoDB connection string (required)
- --start-at "2006-01-02 15:04:05": Start at wallTime (UTC). Mutually exclusive with --resume-token
- --resume-token string: Resume token (_id._data) from a previous event
- --output, -o: Output format: json | table (default: json)
- --include-event-id: Include event _id in table output
- --show-full-document: Do not truncate fullDocument in table output
- --limit int: Stop after N events
- --pipeline string: Single-stage Extended JSON (e.g. {"$match":{"operationType":"insert"}})

Notes:
- --start-at and --resume-token cannot be used together.
- --start-at expects UTC time using layout 2006-01-02 15:04:05.
- --pipeline currently accepts a single stage document in Extended JSON.

## Examples

- Stream to table output (truncated documents):
```bash
mongostream-cli stream \
  --database app --collection users \
  --url "mongodb://localhost:27017" \
  --output table
```

- Start from a wall time (UTC):
```bash
mongostream-cli stream \
  --database app --collection users \
  --url "mongodb://localhost:27017" \
  --output table \
  --start-at "2025-01-12 00:00:00"
```

- Filter only inserts and replaces (single-stage $match):
```bash
mongostream-cli stream \
  --database app --collection users \
  --url "mongodb://localhost:27017" \
  --output table \
  --pipeline '{"$match":{"$or":[{"operationType":"insert"},{"operationType":"replace"}]}}'
```

- Limit to first 5 events and include event id:
```bash
mongostream-cli stream \
  --database app --collection users \
  --url "mongodb://localhost:27017" \
  --output table \
  --limit 5 \
  --include-event-id
```

- Resume from a previous event:
  - Find the event’s resume token in the event _id._data (use --include-event-id in table mode or parse JSON output).
  - Pass that string to --resume-token:
```bash
mongostream-cli stream \
  --database app --collection users \
  --url "mongodb://localhost:27017" \
  --resume-token "<_data string from event _id>"
```

## Output

- json: Prints the raw change event as BSON/Extended JSON.
- table:
  - Columns: [wallTime, operationType, documentKey, fullDocument (truncated to 100 chars)]
  - With --include-event-id, the event _id (resume token document) is prepended.
  - With --show-full-document, the fullDocument is not truncated.

## Internals

- Command wiring: [`stream.GetCmd`](commands/stream/stream.go) is added to the app in [main.go](main.go).
- Event decoding: the change event is unmarshaled into [`stream.StreamEvent`](commands/stream/stream.go) for table output.
- Resume behavior:
  - --start-at sets StartAtOperationTime.
  - --resume-token sets ResumeAfter using the provided _id._data
