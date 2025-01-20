# mongocli


```bash
mongostream-cli stream --database databasename  --collection users  --url <someurl> --o table  --start-at "2025-01-12 00:00:00"
```

```bash
mongostream-cli stream -h                                                                                                                                    1 ↵ main 
NAME:
   mongocli stream - Stream related commands

USAGE:
   mongocli stream [command [command options]]

OPTIONS:
   --database value          Database name: dbmame
   --collection value        Collection name: mycollection
   --url value               mongoURL: mongodb://localhost:27017
   --start-at value          Start at timestamp:2006-01-02 15:04:05
   --resume-token value      Resume token
   --output value, -o value  Output format: json , table (default: "json")
   --include-event-id        Include event id (default: false)
   --show-full-document      Show full document (default: false)
   --limit value             Limit the number of documents (default: 0)
   --help, -h                show help
```
