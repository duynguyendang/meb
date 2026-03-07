# MEB API Server

A REST API server for the MEB (Mangle Extension for Badger) knowledge graph database.

## Quick Start

```bash
# Build the API server
cd meb
go build -o cmd/api/api ./cmd/api/

# Run the server
./cmd/api/api -data ./data -port 8080
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/stats` | GET | Get graph statistics |
| `/v1/facts` | GET | Query facts with filters |
| `/v1/documents` | GET | List all documents |
| `/v1/documents/{id}` | GET | Get document content and metadata |
| `/v1/predicates` | GET | List available predicates |
| `/v1/graphs` | GET | List available graphs |
| `/v1/content/{id}` | GET | Get raw content |
| `/v1/vectors/search` | POST | Vector similarity search |
| `/health` | GET | Health check |

## Examples

### Get Stats

```bash
curl http://localhost:8080/v1/stats
```

Response:
```json
{
  "totalFacts": 12345,
  "totalDocuments": 100,
  "totalVectors": 50,
  "graphs": ["default", "metadata"],
  "predicates": ["triples"]
}
```

### Query Facts

```bash
curl "http://localhost:8080/v1/facts?subject=alice&limit=10"
```

### List Documents

```bash
curl http://localhost:8080/v1/documents
```

### Vector Search

```bash
curl -X POST http://localhost:8080/v1/vectors/search \
  -H "Content-Type: application/json" \
  -d '{"query": "your embedding query", "limit": 10}'
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | 8080 | HTTP server port |
| `-data` | ./data | MEB data directory |
