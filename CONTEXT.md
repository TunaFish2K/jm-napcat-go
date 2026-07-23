# JM Napcat Go Context

## Behavior source

The current TypeScript runtime is the compatibility source of truth. The old
README and the removed HTTP layer are not part of the contract.

## Locked decisions

- Bot-only runtime; no HTTP server.
- NapCat is reached through an active, forward WebSocket connection.
- The selected MIT OneBot SDK supplies typed protocol entities. Its current
  client transport is reverse/Universal-oriented, so forward transport is
  isolated in a local adapter that reuses the typed entities.
- Runtime configuration is a strict, flat camelCase `config.json` in the
  process current directory. Missing configuration is generated and the
  process exits.
- Relative cache paths are resolved from the process current directory.
- Go owns a new cache manifest format under `./cache/info` and `./cache/pdf`;
  old TypeScript cache compatibility is intentionally not supported.
- PDF encryption is pure Go AES-256 through pdfcpu. The ID is both passwords;
  reader compatibility matters, not byte-for-byte qpdf output.
- Tests are unit tests only. Live NapCat and upstream connections are not
  required by CI.
- Release builds contain one Linux amd64 executable, with no archive or
  checksum asset.

## Domain terms

- A photo response supplies the image list and scramble parameters.
- An album response supplies optional descriptive metadata.
- A PDF task moves through `pending`, `processing`, `ready`, or `error`.
- Image progress counts successfully embedded PDF pages, not downloaded files.
