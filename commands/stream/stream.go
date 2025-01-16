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
				Value: time.Now(),
				Config: cli.TimestampConfig{
					Timezone: time.UTC,
					Layouts:  []string{time.DateTime},
				},
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output format: json , table",
				Value:   "json",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			url := c.String("url")
			databaseName := c.String("database")
			collectionName := c.String("collection")
			pipelineFlag := c.String("pipeline")
			startAt := c.Timestamp("start-at")
			opts := options.Client().ApplyURI(url)
			output := c.String("output")
			mongoclient, err := mongo.Connect(ctx, opts)
			if err != nil {
				log.Fatal(err)
			}

			defer mongoclient.Disconnect(ctx)
			pipeline := bson.Raw([]byte(pipelineFlag))
			collection := mongoclient.Database(databaseName).Collection(collectionName)
			streamOpts := options.ChangeStream()
			ts := &primitive.Timestamp{T: uint32(startAt.Unix()), I: 1}
			streamOpts.SetStartAtOperationTime(ts)
			stream, err := collection.Watch(ctx, pipeline, streamOpts)
			if err != nil {
				log.Fatal(err)
			}

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
					if len(docString) > 100 {
						docString = event.FullDocument.String()[:100] + "..."
					}

					row := table.Row{clusterTime, event.OperationType, event.Ns.Db, event.Ns.Coll, docString}
					t.AppendRow(row)
					t.Render()
				default:
					fmt.Println("Invalid output format")
				}
			}
			return nil
		},
	}
}

type StreamEvent struct {
	// ID            primitive.ObjectID `bson:"_id"`
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
