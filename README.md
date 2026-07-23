# jm-napcat-go

Single-binary Go rewrite of the JMComic NapCat bot.

## Run

Build and run from the directory in which the bot should keep its cache:

```sh
go build -trimpath -ldflags='-s -w' -o jm-napcat-go ./src
./jm-napcat-go
```

On the first run the program creates a strict `config.json` with all fields
and exits. Edit the file and run the binary again. Configuration is never
read from environment variables.

The executable actively connects to the NapCat forward WebSocket configured
by `napcatWsUrl`. It also contacts the configured JM upstream service to
query metadata and download images.

The command behavior is preserved from the TypeScript runtime:

- `/query`, `/查询`, `/本子` query information.
- `/pdf`, `/download`, `/dl`, `/下载` generate and send a password-protected PDF.
- `/help`, `/帮助`, and `/?` show help.

See [`config.schema.json`](config.schema.json) for the complete configuration
shape and [`CONTEXT.md`](CONTEXT.md) for compatibility decisions.
