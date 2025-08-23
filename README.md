# mongo-es

MongoDB to Elasticsearch data sync with field mapping support.

## What it does

Watches MongoDB collections and syncs data to Elasticsearch with customizable field transformations. Useful when you don't have access to MongoDB replication.

## Field Mapping System

The tool uses two types of mappers to transform your data:

### 1. MongoDB Mappers (`mappers/`)

Transform MongoDB field names before processing.

**Example: `mappers/users.json`**

```json
{
  "name": "firstName",
  "last_name": "lastName",
  "stats.country": "user_country"
}
```

**Input MongoDB document:**

```json
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John",
  "last_name": "Doe",
  "stats": {
    "country": "US"
  }
}
```

**After MongoDB mapping:**

```json
{
  "_id": "507f1f77bcf86cd799439011",
  "firstName": "John",
  "lastName": "Doe",
  "user_country": "US"
}
```

### 2. Elasticsearch Mappers (`es-mappers/`)

Transform fields for specific Elasticsearch indices.

**Example: `es-mappers/users.json`**

```json
{
  "firstName": "first_name",
  "lastName": "last_name",
  "user_country": "location"
}
```

**Final Elasticsearch document:**

```json
{
  "_id": "507f1f77bcf86cd799439011",
  "first_name": "John",
  "last_name": "Doe",
  "location": "US"
}
```

## Environment Variables

```bash
MONGO_URI=mongodb://localhost:27017
MONGO_DB=your_database
ES_URL=http://localhost:9200
SELECTED_COLLS=users,orders  # comma-separated, or * for all
ES_COLL=users:user_index,orders:order_index  # mongo_collection:es_index
```

## Usage

1. Create mapper files in `mappers/` and `es-mappers/` directories
2. Set environment variables
3. Run: `go run main.go`

The tool will:

- Watch specified MongoDB collections
- Apply MongoDB field mappings
- Apply Elasticsearch index mappings
- Sync transformed data to Elasticsearch

## Mapper File Rules

- Files must be valid JSON
- File name (without .json) matches collection/index name
- Supports nested fields using dot notation: `"stats.country": "user_country"`
- If no mapper exists, data passes through unchanged
