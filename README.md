# MongoDB::Fast

A high-performance, async MongoDB driver for Raku with an optimized BSON and wire protocol implementation. Designed for speed and simplicity, with full support for single connections and connection pools.

## Features

- Async/await API using native Raku `Promise`
- Persistent background socket reader (no per-request reconnect overhead)
- Batched inserts via OP_MSG document sequences
- Connection pooling
- Auto-reconnect with exponential backoff
- CRUD: insert, find, update, delete, replace
- Aggregation pipeline
- Index management
- Cursor support with lazy pagination

## Installation

```bash
zef install MongoDB::Fast
```

Or clone and install locally:

```bash
git clone https://github.com/fastmongo/fastmongo.git
cd fastmongo
zef install .
```

## Quick Start

```raku
use MongoDB::Fast;

my $m = MongoDB::Fast.new;
await $m.connect;

my $col = $m.db('mydb').collection('users');

# Insert
await $col.insert-one({ name => 'Alice', age => 30 });

# Find one
my $doc = await $col.find-one({ name => 'Alice' });
say $doc<name>;  # Alice

# Clean up
$m.close;
```

## Connection Options

```raku
my $m = MongoDB::Fast.new(
    host                   => '127.0.0.1',   # default
    port                   => 27017,          # default
    use-pool               => False,          # set True to use connection pool
    max-connections        => 10,             # pool size
    enable-auto-reconnect  => True,
    max-reconnect-attempts => 5,
    initial-retry-delay    => 0.1e0,          # seconds, doubles each attempt
    connection-timeout     => 10,             # seconds
);
await $m.connect;
```

## CRUD Operations

### Insert

```raku
# Insert one document — returns { insertedId => ..., acknowledged => True }
my $r = await $col.insert-one({ name => 'Bob', score => 42 });
say $r<insertedId>;

# Insert many documents — returns { insertedIds => [...], insertedCount => N }
my @docs = ({ name => 'Alice' }, { name => 'Bob' }, { name => 'Carol' });
my $r = await $col.insert-many(@docs);
say $r<insertedCount>;  # 3
```

### Find

```raku
# Find one document
my $doc = await $col.find-one({ name => 'Alice' });

# Find one with projection and sort
my $doc = await $col.find-one(
    { status => 'active' },
    options => { sort => { score => -1 }, projection => { name => 1, score => 1 } }
);

# Cursor — lazy, paginated
my $cursor = $col.find({ status => 'active' });
while my $doc = await $cursor.next {
    say $doc<name>;
}

# Fetch all at once (uses large batchSize internally)
my @all = await $col.find({ status => 'active' }).all;

# Find with options
my $cursor = $col.find(
    { age => { '$gte' => 18 } },
    options => { sort => { age => 1 }, limit => 50, batchSize => 100 }
);
```

### Update

```raku
# Update one
my $r = await $col.update-one(
    { name => 'Alice' },
    { '$set' => { score => 99 } }
);
say $r<modifiedCount>;

# Update many
my $r = await $col.update-many(
    { status => 'inactive' },
    { '$set' => { archived => True } }
);

# Upsert
await $col.update-one(
    { name => 'Dave' },
    { '$set' => { score => 10 } },
    :upsert
);

# Replace one
await $col.replace-one(
    { name => 'Alice' },
    { name => 'Alice', score => 100, updated => True }
);
```

### Delete

```raku
# Delete one
my $r = await $col.delete-one({ name => 'Bob' });
say $r<deletedCount>;

# Delete many
my $r = await $col.delete-many({ score => { '$lt' => 10 } });
```

### Count

```raku
my $n = await $col.count-documents;            # all
my $n = await $col.count-documents({ age => { '$gte' => 18 } });
```

### Aggregation

```raku
# aggregate returns an Array — receive it in a scalar, then iterate
my $results = await $col.aggregate([
    { '$match'  => { status => 'active' } },
    { '$group'  => { _id => '$country', total => { '$sum' => 1 } } },
    { '$sort'   => { total => -1 } },
    { '$limit'  => 10 },
]);

for @$results -> $r {
    say "$r<_id>: $r<total>";
}
```

### Indexes

```raku
# Create a single-field index
await $col.create-index({ email => 1 });

# Unique index with custom name
await $col.create-index(
    { email => 1 },
    options => { unique => True, name => 'email_unique' }
);

# Compound index
await $col.create-index({ country => 1, score => -1 });
```

## Database Operations

```raku
my $db = $m.db('mydb');

# List collections
my @cols = await $db.list-collections;
say @cols.map(*<name>).join(', ');

# Database stats
my $stats = await $db.stats;
say $stats<dataSize>;

# Drop database
await $db.drop;
```

## Collection Operations

```raku
# Drop collection
await $col.drop;
```

## Connection Pool

```raku
my $m = MongoDB::Fast.new(:use-pool, max-connections => 20);
await $m.connect;

# All subsequent operations automatically acquire/release connections
my $col = $m.db('mydb').collection('events');
await $col.insert-one({ type => 'login', user => 'alice' });

$m.close;  # closes all pooled connections
```

## Health Check

```raku
my $ok = await $m.ping;
say $ok ?? "MongoDB is up" !! "MongoDB is down";
```

## Bulk Insert Example

For high-throughput ingestion, batch documents and insert in chunks:

```raku
use MongoDB::Fast;

sub MAIN(Int $total = 10_000, Int $batch-size = 1_000) {
    my $m = MongoDB::Fast.new;
    await $m.connect;
    my $col = $m.db('bench').collection('data');

    my $start = now;
    my @batch;

    for ^$total -> $i {
        @batch.push: { index => $i, value => $i * $i };
        if $i > 0 && $i %% $batch-size {
            await $col.insert-many(@batch);
            @batch = ();
        }
    }
    await $col.insert-many(@batch) if @batch;

    my $elapsed = now - $start;
    say "Inserted $total docs in {$elapsed.fmt('%.3f')}s ({($total / $elapsed).Int} docs/sec)";

    await $col.drop;
    $m.close;
}
```

## Error Handling

All methods return `Promise`. Use `try`/`CATCH` to handle errors:

```raku
my $doc = await $col.find-one({ _id => 'missing' });
say $doc.defined ?? $doc<name> !! "not found";

try {
    await $col.insert-one({ _id => 'dup' });
    await $col.insert-one({ _id => 'dup' });
    CATCH {
        default { say "Error: {$_.message}" }
    }
}
```

## Architecture

| Module | Role |
|--------|------|
| `MongoDB::Fast` | Top-level client; holds connection or pool |
| `MongoDB::Fast::Connection` | Single TCP connection, background reader, serialized send/recv |
| `MongoDB::Fast::Connection::Pool` | Pool of connections with acquire/release |
| `MongoDB::Fast::Database` | Database handle; dispatches commands |
| `MongoDB::Fast::Collection` | CRUD and aggregation methods |
| `MongoDB::Fast::Cursor` | Lazy paginated result set |
| `MongoDB::Fast::Wire` | OP_MSG framing and parsing |
| `MongoDB::Fast::BSON` | BSON encode/decode |
| `MongoDB::Fast::ObjectID` | ObjectID generation |

## License

Artistic-2.0
