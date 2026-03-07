# MEB GUI

A web-based graphical user interface for exploring and visualizing the MEB (Mangle Extension for Badger) knowledge graph database.

## Features

- **Facts Browser** - Query and browse facts (Subject-Predicate-Object triples) with filtering
- **Document Store** - View stored documents and their content
- **Vector Search** - Semantic similarity search using embeddings
- **Dashboard** - Overview of graph statistics

## Quick Start

### 1. Start the API Server

```bash
# Build the API server
cd meb
go build -o cmd/api/api ./cmd/api/

# Run the API server
./cmd/api/api -data ./data -port 8080
```

### 2. Start the GUI

```bash
# Install dependencies
cd gui
npm install

# Start development server
npm run dev
```

The GUI will be available at `http://localhost:3000`.

### 3. Configure the GUI

By default, the GUI connects to `http://localhost:8080/v1`. You can change this in the Settings modal (click the Settings button in the sidebar).

## API Requirements

The GUI expects a MEB API server with the following endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/stats` | GET | Get graph statistics |
| `/facts` | GET | Query facts with filters |
| `/documents` | GET | List all documents |
| `/documents/:id` | GET | Get document content |
| `/vectors/search` | POST | Vector similarity search |

## Project Structure

```
gui/
├── src/
│   ├── components/    # React components
│   ├── hooks/         # Custom React hooks
│   ├── services/      # API client
│   ├── types/         # TypeScript types
│   ├── App.tsx        # Main app component
│   └── main.tsx       # Entry point
├── package.json
└── vite.config.ts
```

## Tech Stack

- React 18
- TypeScript
- Vite
- Lucide React (icons)
