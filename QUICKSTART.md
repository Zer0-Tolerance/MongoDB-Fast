# MongoDB::Fast Quick Start Guide

## Installation

Currently, install from source:

```bash
cd MongoDB::Fast
zef install .
```

## Testing Without MongoDB

You can test the BSON encoding/decoding functionality without MongoDB:

```bash
raku -I lib examples/bson-demo.raku
```

This demonstrates:
- Encoding and decoding documents
- All supported BSON data types
- ObjectID generation
- Performance metrics

## Testing With MongoDB

### 1. Start MongoDB

Using Docker (easiest):

```bash
docker run -d -p 27017:27017 --name fastmongo-test mongo:latest
```

Or use your existing MongoDB installation (ensure it's running on localhost:27017).

### 2. Run Basic Example

```bash
raku -I lib examples/basic-usage.raku
```

This will:
- Connect to MongoDB
- Create a test database and collection
- Insert documents (single and bulk)
- Query documents with filters
- Update documents
- Delete documents
- Run aggregations
- Clean up

### 3. Run Benchmark

```bash
raku -I lib examples/benchmark.raku --documents=1000
```

Add `--pool` flag to test with connection pooling:

```bash
raku -I lib examples/benchmark.raku --documents=1000 --pool
```

### 4. Advanced Queries

```bash
raku -I lib examples/advanced-queries.raku
```

Demonstrates:
- Complex filters
- Array queries
- Nested document queries
- Aggregation pipelines
- Sorting and limiting

### 5. Connection Pooling

```bash
raku -I lib examples/connection-pool.raku
```

Shows concurrent operations with connection pooling.

## Running Tests

```bash
# Test BSON encoder/decoder
raku -I lib t/01-bson.rakutest

# Test wire protocol
raku -I lib t/02-wire.rakutest
```

## Basic Usage in Your Code

```raku
use MongoDB::Fast;

# Create client
my $client = MongoDB::Fast.new(
    host => 'localhost',
    port => 27017,
);

# Connect
await $client.connect;

# Get database and collection
my $db = $client.db('mydb');
my $col = $db.collection('users');

# Insert
my $result = await $col.insert-one({
    name => 'Alice',
    age => 30,
});

# Find
my $cursor = $col.find({ age => { '$gte' => 25 } });
my @users = await $cursor.all;

# Update
await $col.update-one(
    { name => 'Alice' },
    { '$set' => { age => 31 } }
);

# Delete
await $col.delete-one({ name => 'Alice' });

# Close
$client.close;
```

## Current Limitations

1. **Authentication**: SCRAM-SHA-256 authentication is under development. Use MongoDB without authentication for now.

2. **Dependencies**: Requires NativeCall for BSON encoding/decoding (part of Raku core).

## Performance Tips

1. **Use Connection Pooling** for high-concurrency applications:
   ```raku
   my $client = MongoDB::Fast.new(
       host => 'localhost',
       port => 27017,
       use-pool => True,
       max-connections => 10,
   );
   ```

2. **Use Bulk Operations** when inserting many documents:
   ```raku
   await $col.insert-many(@documents);
   ```

3. **Use Projections** to limit returned fields:
   ```raku
   my $cursor = $col.find(
       { age => { '$gte' => 25 } },
       options => { projection => { name => 1, age => 1 } }
   );
   ```

4. **Use Indexes** for frequently queried fields:
   ```raku
   await $col.create-index({ email => 1 }, options => { unique => True });
   ```

## Troubleshooting

### Connection Refused

Make sure MongoDB is running:
```bash
docker ps  # Check if container is running
# or
mongosh  # Try connecting with mongo shell
```

### Module Not Found

Use `-I lib` flag when running examples:
```bash
raku -I lib examples/basic-usage.raku
```

### Performance Issues

1. Enable connection pooling
2. Use bulk operations
3. Create appropriate indexes
4. Use projections to limit data transfer

## Next Steps

- Read the full [README.md](README.md) for detailed API documentation
- Explore examples in the `examples/` directory
- Check out the tests in `t/` for more usage patterns

## Getting Help

- GitHub Issues: https://github.com/fastmongo/fastmongo/issues
- Raku Community: https://raku.org/community/

Happy coding with MongoDB::Fast!
