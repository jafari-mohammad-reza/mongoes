# mongo-es

MongoDB to Elasticsearch data sync with YAML-based field mapping support.

## What it does

Watches MongoDB collections and syncs data to Elasticsearch with customizable field transformations using a centralized YAML configuration system. Useful when you don't have access to MongoDB replication.

## Configuration Structure

The tool uses two main configuration files:

### 1. `config.yaml

` - System Configuration

Defines MongoDB and Elasticsearch connection settings and collection handling rules.

**Example:**

```yaml
mongo:
  url: mongodb://localhost:27018
  db: your_database
  batch_timeout: 10
  white_list:
    - users
    - products
    - orders
  coll_batch:
    users: 50
    products: 100

elastic:
  addresses:
    - http://localhost:9200
  user: elastic_user
  password: elastic_password
  unique_fields:
    user_index: _id
    product_index: productId
  indic_period:
    user_index: 24
  coll_prefix:
    users: user_index
    products: product_index
```

### 2. `mappings.yaml` - Field Mapping Rules

Defines how fields are transformed during the sync process with two mapping stages:

**Example:**

```yaml
# MongoDB field mappings (applied first)
mongo:
  users:
    _id: id
    name: firstName
    email: userEmail
    stats.country: userCountry
  products:
    _id: productId
    title: productName
    price: cost

# Elasticsearch field mappings (applied second)
elastic:
  user_index:
    id: _id
    firstName: name
    userEmail: email
    userCountry: location
  product_index:
    productId: _id
    productName: title
    cost: price
```

## How Field Mapping Works

The system applies transformations in two stages:

### Stage 1: MongoDB Mapping

Transforms MongoDB document fields using the `mongo` section of `mappings.yaml`.

**Input MongoDB document:**

```json
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John",
  "email": "john@example.com",
  "stats": {
    "country": "US"
  }
}
```

**After MongoDB mapping (users collection):**

```json
{
  "id": "507f1f77bcf86cd799439011",
  "firstName": "John",
  "userEmail": "john@example.com",
  "userCountry": "US"
}
```

### Stage 2: Elasticsearch Index Mapping

Transforms fields for the target Elasticsearch index using the `elastic` section.

**Final Elasticsearch document (user_index):**

```json
{
  "_id": "507f1f77bcf86cd799439011",
  "name": "John",
  "email": "john@example.com",
  "location": "US"
}
```

## Configuration Options

### MongoDB Configuration

- `url`: MongoDB connection string
- `db`: Database name to watch
- `batch_timeout`: Timeout in seconds for batch processing
- `white_list`: Array of collection names to sync (only these will be processed)
- `coll_batch`: Custom batch sizes per collection (default: 100)

### Elasticsearch Configuration

- `addresses`: Array of Elasticsearch node URLs
- `user`: Elasticsearch username (optional)
- `password`: Elasticsearch password (optional)
- `unique_fields`: Unique field name per index (default: "\_id")
- `indic_period`: Index period settings per index (default: 24)
- `coll_prefix`: Maps MongoDB collection names to Elasticsearch index names

## Usage

1. **Create configuration files:**
   - `config.yaml` - System and connection settings
   - `mappings.yaml` - Field transformation rules

2. **Build and run:**

   ```bash
   make build_linux_amd_64  # Build for Linux
   # or
   make run                 # Run with hot reload using air
   # or
   go run main.go           # Run directly
   ```

3. **The tool will:**
   - Connect to MongoDB and Elasticsearch
   - Watch specified collections from the white_list
   - Apply MongoDB field mappings from `mappings.yaml`
   - Apply Elasticsearch index mappings
   - Sync transformed data to the corresponding Elasticsearch indices

## Mapping Rules

### MongoDB Mappings (`mongo` section)

- Organized by collection name
- Supports nested fields using dot notation: `"stats.country": "userCountry"`
- Applied before Elasticsearch mappings

### Elasticsearch Mappings (`elastic` section)

- Organized by Elasticsearch index name (as defined in `coll_prefix`)
- Applied to the output of MongoDB mappings
- Handles complex nested objects and arrays

### Key Features

- **Nested field support**: Use dot notation for nested MongoDB fields
- **Array handling**: Automatically processes arrays and nested objects
- **Flexible mapping**: Collections without mappings pass through unchanged
- **Centralized configuration**: All mappings in one YAML file
- **Type preservation**: Maintains data types during transformation

## Example Complete Setup

**config.yaml:**

```yaml
mongo:
  url: mongodb://localhost:27018
  db: ecommerce
  white_list:
    - users
    - orders

elastic:
  addresses:
    - http://localhost:9200
  coll_prefix:
    users: user_index
    orders: order_index
```

**mappings.yaml:**

```yaml
mongo:
  users:
    _id: userId
    profile.name: userName
    profile.email: userEmail
    settings.preferences.country: userCountry

elastic:
  user_index:
    userId: _id
    userName: name
    userEmail: email
    userCountry: location
```

This setup will sync the `users` collection to the `user_index` Elasticsearch index with the specified field transformations.
