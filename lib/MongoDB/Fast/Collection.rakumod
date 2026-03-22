use v6.d;
use MongoDB::Fast::Connection;
use MongoDB::Fast::Cursor;
use MongoDB::Fast::BSON;

unit class MongoDB::Fast::Collection;

has Str $.name;
has Str $.database;
has MongoDB::Fast::Connection $.connection;
has $.pool;  # Can be MongoDB::Fast::Connection::Pool
has MongoDB::Fast::BSON $.bson;

submethod BUILD(
    Str :$!name,
    Str :$!database,
    MongoDB::Fast::Connection :$!connection,
    :$!pool
) {
    $!bson = MongoDB::Fast::BSON.new;
}

# Helper method to run command with either connection or pool
method !run-command(Hash $command --> Promise) {
    if $!pool {
        return start {
            my $conn = await $!pool.acquire;
            LEAVE $!pool.release($conn) if $conn;
            await $conn.run-command($command, $!database);
        };
    } else {
        return $!connection.run-command($command, $!database);
    }
}

# Helper method to run command with document sequence (pool-aware)
method !run-command-with-docs(Hash $command, Str $identifier, @documents --> Promise) {
    if $!pool {
        return start {
            my $conn = await $!pool.acquire;
            LEAVE $!pool.release($conn) if $conn;
            await $conn.run-command-with-docs($command, $identifier, @documents, $!database);
        };
    } else {
        return $!connection.run-command-with-docs($command, $identifier, @documents, $!database);
    }
}

# Find documents
method find(Hash $filter = {}, Hash :$options = {} --> MongoDB::Fast::Cursor) {
    return MongoDB::Fast::Cursor.new(
        connection => $!connection,
        pool => $!pool,
        database => $!database,
        collection => $!name,
        filter => $filter,
        options => $options,
    );
}

# Find one document (direct command, bypasses Cursor machinery)
method find-one(Hash $filter = {}, Hash :$options = {} --> Promise) {
    return start {
        my %command = %(
            find       => $!name,
            filter     => $filter,
            limit      => 1,
            batchSize  => 1,
        );
        %command<skip>       = $options<skip>       if $options<skip>:exists;
        %command<sort>       = $options<sort>       if $options<sort>:exists;
        %command<projection> = $options<projection> if $options<projection>:exists;

        my $response = await self!run-command(%command);
        ($response<cursor><firstBatch> // [])[0];
    };
}

# Insert one document
method insert-one(Hash $document --> Promise) {
    return start {
        # Generate ObjectID early if needed (outside async critical path)
        my %doc = %$document;
        unless %doc<_id>:exists {
            %doc<_id> = $!bson.generate-object-id;
        }

        # Use document sequence for consistency with insert-many
        my %command = %(
            insert => $!name,
            ordered => True,
        );

        my $response = await self!run-command-with-docs(
            %command,
            'documents',
            [$(%doc)]
        );
        # dd $response;
        if $response<ok> && !$response<writeErrors> {
            {
                insertedId => %doc<_id>,
                acknowledged => True,
            }
        } elsif $response<writeErrors> {
            my $err = $response<writeErrors>[0];
            die "Insert failed: {$err<errmsg> // $err<keyValue> // 'write error'}";
        } else {
            die "Insert failed: {$response<errmsg> // 'unknown error'}";
        }
    };
}

# Insert many documents
method insert-many(@documents, Bool :$ordered = True --> Promise) {
    my @snap = @documents;  # snapshot before async — caller may mutate @documents
    return start {
        # Batch prepare all documents with ObjectID generation
        my @docs;
        for @snap -> $doc {
            if $doc<_id>:exists {
                @docs.push: $doc;
            } else {
                my %d = %$doc;
                %d<_id> = $!bson.generate-object-id;
                @docs.push: %d;
            }
        }

        # Use document sequence for efficiency
        my %command = %(
            insert => $!name,
            ordered => $ordered,
        );

        my $response = await self!run-command-with-docs(
            %command,
            'documents',
            @docs
        );

        if $response<ok> {
            if $response<writeErrors> {
                my $err = $response<writeErrors>[0];
                die "Insert many failed: {$err<errmsg> // 'write error at index {$err<index>}'}";
            }
            {
                insertedIds => @docs.map(*<_id>),
                insertedCount => $response<n>,
                acknowledged => True,
            };
        } else {
            die "Insert many failed: {$response<errmsg> // 'unknown error'}";
        }
    };
}

# Update one document
method update-one(Hash $filter, Hash $update, Bool :$upsert = False --> Promise) {
    return start {
        my %command = %(
            update => $!name,
            updates => [
                $({
                    q => $filter,
                    u => $update,
                    upsert => $upsert,
                    multi => False,
                })
            ],
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            {
                matchedCount => $response<n>,
                modifiedCount => $response<nModified> // 0,
                upsertedId => $response<upserted>:exists ?? $response<upserted>[0]<_id> !! Nil,
                acknowledged => True,
            };
        } else {
            die "Update failed: {$response<errmsg>}";
        }
    };
}

# Update many documents
method update-many(Hash $filter, Hash $update, Bool :$upsert = False --> Promise) {
    return start {
        my %command = %(
            update => $!name,
            updates => [
                $({
                    q => $filter,
                    u => $update,
                    upsert => $upsert,
                    multi => True,
                })
            ],
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            {
                matchedCount => $response<n>,
                modifiedCount => $response<nModified> // 0,
                upsertedId => $response<upserted>:exists ?? $response<upserted>[0]<_id> !! Nil,
                acknowledged => True,
            };
        } else {
            die "Update many failed: {$response<errmsg>}";
        }
    };
}

# Replace one document
method replace-one(Hash $filter, Hash $replacement, Bool :$upsert = False --> Promise) {
    return start {
        my %command = %(
            update => $!name,
            updates => [
                $({
                    q => $filter,
                    u => $replacement,
                    upsert => $upsert,
                    multi => False,
                })
            ],
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            {
                matchedCount => $response<n>,
                modifiedCount => $response<nModified> // 0,
                upsertedId => $response<upserted>:exists ?? $response<upserted>[0]<_id> !! Nil,
                acknowledged => True,
            };
        } else {
            die "Replace failed: {$response<errmsg>}";
        }
    };
}

# Delete one document
method delete-one(Hash $filter --> Promise) {
    return start {
        my %command = %(
            delete => $!name,
            deletes => [
                $({
                    q => $filter,
                    limit => 1,
                })
            ],
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            {
                deletedCount => $response<n>,
                acknowledged => True,
            };
        } else {
            die "Delete failed: {$response<errmsg>}";
        }
    };
}

# Delete many documents
method delete-many(Hash $filter --> Promise) {
    return start {
        my %command = %(
            delete => $!name,
            deletes => [
                $({
                    q => $filter,
                    limit => 0,
                })
            ],
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            {
                deletedCount => $response<n>,
                acknowledged => True,
            };
        } else {
            die "Delete many failed: {$response<errmsg>}";
        }
    };
}

# Count documents
method count-documents(Hash $filter = {} --> Promise) {
    return start {
        my %command = %(
            count => $!name,
            query => $filter,
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            $response<n>;
        } else {
            die "Count failed: {$response<errmsg>}";
        }
    };
}

# Aggregate
method aggregate(@pipeline, Hash :$options = {} --> Promise) {
    return start {
        my %command = %(
            aggregate => $!name,
            pipeline => @pipeline,
            cursor => { batchSize => $options<batchSize> // 100 },
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            my $cursor = $response<cursor>;
            $cursor<firstBatch> // [];
        } else {
            die "Aggregate failed: {$response<errmsg>}";
        }
    };
}

# Create index
method create-index(Hash $keys, Hash :$options = {} --> Promise) {
    return start {
        my %index = %(
            key  => $keys,
            name => $options<name> // $keys.keys.join('_'),
            |%$options,
        );

        my %command = %(
            createIndexes => $!name,
            indexes => [$(%index)],
        );

        my $response = await self!run-command(%command);

        if $response<ok> {
            True;
        } else {
            die "Create index failed: {$response<errmsg>}";
        }
    };
}

# Drop collection
method drop(--> Promise) {
    return start {
        my %command = %(
            drop => $!name,
        );

        my $response = await self!run-command(%command);

        # Drop returns ok even if collection doesn't exist
        True;
    };
}
