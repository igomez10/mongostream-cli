package stream

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetCmd() *cli.Command {
	return &cli.Command{
		Name:  "stream",
		Usage: "Stream related commands",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database",
				Usage:    "Database name: dbmame",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "collection",
				Usage:    "Collection name: mycollection",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "url",
				Usage:    "mongoURL: mongodb://localhost:27017",
				Required: true,
			},
			&cli.TimestampFlag{
				Name:  "start-at",
				Usage: "Start at timestamp:" + time.DateTime,
				Config: cli.TimestampConfig{
					Timezone: time.UTC,
					Layouts:  []string{time.DateTime},
				},
			},
			&cli.StringFlag{
				Name:  "resume-token",
				Usage: "Resume token",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output format: json , table",
				Value:   "json",
			},
			&cli.BoolFlag{
				Name:  "include-event-id",
				Usage: "Include event id",
				Value: false,
			}, &cli.BoolFlag{
				Name:  "show-full-document",
				Usage: "Show full document",
				Value: false,
			},
			&cli.IntFlag{
				Name:        "limit",
				Usage:       "Limit the number of documents",
				HideDefault: true,
			},
			&cli.StringFlag{
				Name:        "pipeline",
				Usage:       `Pipeline: {"$match":{"$or":[{"operationType":"insert"},{"operationType":"replace"}]}}`,
				HideDefault: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {

			// set log stack trace
			log.SetFlags(log.LstdFlags | log.Lshortfile)

			url := c.String("url")
			databaseName := c.String("database")
			collectionName := c.String("collection")
			startAt := c.Timestamp("start-at")
			pipelineFlag := c.String("pipeline")
			resumeToken := c.String("resume-token")
			includeEventID := c.Bool("include-event-id")
			showFullDocument := c.Bool("show-full-document")
			limit := c.Int("limit")
			opts := options.Client().ApplyURI(url)
			output := c.String("output")

			if !startAt.IsZero() && c.String("resume-token") != "" {
				log.Fatal("Cannot use both start-at and resume-token")
			}

			mongoclient, err := mongo.Connect(ctx, opts)
			if err != nil {
				log.Fatal(err)
			}

			defer mongoclient.Disconnect(ctx)
			collection := mongoclient.Database(databaseName).Collection(collectionName)
			streamOpts := options.ChangeStream()

			if !startAt.IsZero() {
				ts := &primitive.Timestamp{T: uint32(startAt.Unix()), I: 1}
				streamOpts.SetStartAtOperationTime(ts)
			}

			if resumeToken != "" {
				streamOpts.SetResumeAfter(bson.M{"_data": resumeToken})
			}

			pipeline := mongo.Pipeline{}
			if pipelineFlag != "" {
				unmarshaledPipeline := bson.D{}
				if err := bson.UnmarshalExtJSON([]byte(pipelineFlag), false, &unmarshaledPipeline); err != nil {
					log.Fatal(err)
				}
				pipeline = append(pipeline, unmarshaledPipeline)
			}

			stream, err := collection.Watch(ctx, pipeline, streamOpts)
			if err != nil {
				log.Fatal(err)
			}

			counter := 0
			for stream.Next(ctx) {
				current := stream.Current
				switch output {
				case "json":
					fmt.Println(current)
				case "table":
					t := table.NewWriter()
					t.SetOutputMirror(os.Stdout)
					var event StreamEvent
					if err := bson.Unmarshal(current, &event); err != nil {
						log.Fatal(err)
					}

					docString := event.FullDocument.String()
					if len(docString) > 100 && !showFullDocument {
						docString = event.FullDocument.String()[:100] + "..."
					}

					columns := []interface{}{}
					if includeEventID {
						columns = append(columns, event.ID)
					}

					columns = append(columns, event.WallTime.String(), event.OperationType, event.DocumentKey, docString)
					row := table.Row{}
					for i := range columns {
						row = append(row, columns[i])
					}
					t.AppendRow(row)
					t.Render()
				default:
					fmt.Println("Invalid output format")
				}
				counter++
				if counter == int(limit) {
					break
				}
			}
			return nil
		},
	}
}

type EventID struct {
	Data string `bson:"_data"`
}

type StreamEvent struct {
	ID            any       `bson:"_id"`
	WallTime      time.Time `bson:"wallTime"`
	OperationType string    `bson:"operationType"`
	FullDocument  bson.Raw  `bson:"fullDocument"`
	Ns            struct {
		Db   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	DocumentKey any `bson:"documentKey"`
}

func (s StreamEvent) ToTable(includeFullDocument bool) string {
	clusterTime := s.WallTime.String()
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("%s %s %s %s ", clusterTime, s.OperationType, s.Ns.Db, s.Ns.Coll))
	if includeFullDocument {
		if len(s.FullDocument.String()) > 100 {
			builder.WriteString(fmt.Sprintf("%s", s.FullDocument.String()[:100]))
			builder.WriteString(fmt.Sprintf("..."))
			builder.WriteString(fmt.Sprintf("}\n"))
		} else {
			builder.WriteString(fmt.Sprintf("%s\n", s.FullDocument))
		}
	}
	return builder.String()
}
