use v6.d;
use MongoDB::Fast::Connection;
use MongoDB::Fast::Collection;

unit class MongoDB::Fast::Database;

has Str $.name;
has MongoDB::Fast::Connection $.connection;
has $.pool;  # Can be MongoDB::Fast::Connection::Pool

submethod BUILD(
    Str :$!name,
    MongoDB::Fast::Connection :$!connection,
    :$!pool
) { }

# Get collection
method collection(Str $name --> MongoDB::Fast::Collection) {
    if $!pool {
        return MongoDB::Fast::Collection.new(
            name => $name,
            database => $!name,
            pool => $!pool,
        );
    } else {
        return MongoDB::Fast::Collection.new(
            name => $name,
            database => $!name,
            connection => $!connection,
        );
    }
}

# Shorthand for collection
method c(Str $name --> MongoDB::Fast::Collection) {
    return self.collection($name);
}

# Run command
method run-command(Hash $command --> Promise) {
    if $!pool {
        return start {
            my $conn = await $!pool.acquire;
            LEAVE $!pool.release($conn) if $conn;
            await $conn.run-command($command, $!name);
        };
    } else {
        return $!connection.run-command($command, $!name);
    }
}

# List collections
method list-collections(--> Promise) {
    return start {
        my %command = %(
            listCollections => 1,
        );

        my $response = await self.run-command(%command);

        if $response<ok> {
            my $cursor = $response<cursor>;
            $cursor<firstBatch> // [];
        } else {
            die "List collections failed: {$response<errmsg>}";
        }
    };
}

# Drop database
method drop(--> Promise) {
    return start {
        my %command = %(
            dropDatabase => 1,
        );

        my $response = await self.run-command(%command);

        if $response<ok> {
            True;
        } else {
            die "Drop database failed: {$response<errmsg>}";
        }
    };
}

# Get database stats
method stats(--> Promise) {
    return start {
        my %command = %(
            dbStats => 1,
        );

        my $response = await self.run-command(%command);

        if $response<ok> {
            $response;
        } else {
            die "Stats failed: {$response<errmsg>}";
        }
    };
}
