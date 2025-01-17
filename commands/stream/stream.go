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
				Name:  "pipeline",
				Usage: "Pipeline: {}",
				Value: "",
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
				Name:  "limit",
				Usage: "Limit the number of documents",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			url := c.String("url")
			databaseName := c.String("database")
			collectionName := c.String("collection")
			pipelineFlag := c.String("pipeline")
			startAt := c.Timestamp("start-at")
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
			pipeline := bson.Raw([]byte(pipelineFlag))
			collection := mongoclient.Database(databaseName).Collection(collectionName)
			streamOpts := options.ChangeStream()

			if !startAt.IsZero() {
				ts := &primitive.Timestamp{T: uint32(startAt.Unix()), I: 1}
				streamOpts.SetStartAtOperationTime(ts)
			}

			if resumeToken != "" {
				streamOpts.SetResumeAfter(bson.M{"_data": resumeToken})
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

					clusterTime := time.Unix(int64(event.ClusterTime.T), int64(event.ClusterTime.I)).UTC().String()
					docString := event.FullDocument.String()
					if len(docString) > 100 && !showFullDocument {
						docString = event.FullDocument.String()[:100] + "..."
					}

					columns := []interface{}{}
					if includeEventID {
						columns = append(columns, event.ID.Data)
					}

					columns = append(columns, clusterTime, event.OperationType, event.DocumentKey.ID.Hex(), event.Ns.Db, event.Ns.Coll, docString)
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
	ID            EventID  `bson:"_id"`
	OperationType string   `bson:"operationType"`
	FullDocument  bson.Raw `bson:"fullDocument"`
	Ns            struct {
		Db   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	DocumentKey struct {
		ID primitive.ObjectID `bson:"_id"`
	} `bson:"documentKey"`
	ClusterTime primitive.Timestamp `bson:"clusterTime"`
}

func (s StreamEvent) ToTable(includeFullDocument bool) string {
	clusterTime := time.Unix(int64(s.ClusterTime.T), int64(s.ClusterTime.I)).String()
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
