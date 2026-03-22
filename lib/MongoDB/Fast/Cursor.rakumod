use v6.d;
use MongoDB::Fast::Connection;

unit class MongoDB::Fast::Cursor;

has MongoDB::Fast::Connection $.connection;
has $.pool;  # Can be MongoDB::Fast::Connection::Pool
has Str $.database;
has Str $.collection;
has Hash $.filter;
has Hash $.options;
has @.documents;
has Int $.cursor-id = 0;
has Int $.index = 0;
has Bool $.exhausted = False;

submethod BUILD(
    MongoDB::Fast::Connection :$!connection,
    :$!pool,
    Str :$!database,
    Str :$!collection,
    Hash :$!filter = {},
    Hash :$!options = {}
) { }

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

method next(--> Promise) {
    return start {
        # If we have documents in buffer, return next one
        if $!index < @!documents.elems {
            @!documents[$!index++]
        }
        # If cursor is exhausted, return Nil
        elsif $!exhausted {
            Nil
        }
        else {
            # Fetch more documents
            if $!cursor-id == 0 {
                # Initial find
                self!initial-find;
            } else {
                # Get more
                self!get-more;
            }

            # Return next document or Nil
            if $!index < @!documents.elems {
                @!documents[$!index++]
            }
            else {
                Nil
            }
        }
    };
}

method all(--> Promise) {
    return start {
        # all() wants everything: use a large batchSize to collapse multiple
        # round-trips into one (or very few).  Respect an explicit user-set
        # batchSize so callers can still control memory usage if they choose.
        my int $bulk = $!options<batchSize> // 10_000;

        self!initial-find($bulk) unless @!documents || $!exhausted;

        # Collect documents already in buffer
        my @all = @!documents[$!index..*];
        $!index = @!documents.elems;

        # Fetch remaining batches; with bulk=10_000 this is rarely needed
        while !$!exhausted {
            self!get-more($bulk);
            @all.append: @!documents;
            $!index = @!documents.elems;
        }

        @all;
    };
}

method !initial-find(Int $batch-size = ($!options<batchSize> // 100)) {
    my %command = %(
        find   => $!collection,
        filter => $!filter,
    );

    # Add options
    %command<limit>      = $!options<limit>      if $!options<limit>;
    %command<skip>       = $!options<skip>       if $!options<skip>;
    %command<sort>       = $!options<sort>       if $!options<sort>;
    %command<projection> = $!options<projection> if $!options<projection>;
    %command<batchSize>  = $batch-size;

    my $response = await self!run-command(%command);

    if $response<ok> {
        my $cursor = $response<cursor>;
        $!cursor-id = $cursor<id> // 0;
        @!documents = |($cursor<firstBatch> // []);
        $!index = 0;

        # If cursor ID is 0, no more documents
        $!exhausted = True if $!cursor-id == 0;
    } else {
        die "Find failed: {$response<errmsg>}";
    }
}

method !get-more(Int $batch-size = ($!options<batchSize> // 100)) {
    # Build command with proper field ordering
    my %command;
    %command<getMore>     = Int($!cursor-id);
    %command<collection>  = $!collection;
    %command<batchSize>   = $batch-size;

    my $response = await self!run-command(%command);

    if $response<ok> {
        my $cursor = $response<cursor>;
        $!cursor-id = $cursor<id> // 0;
        @!documents = |($cursor<nextBatch> // []);
        $!index = 0;

        # If cursor ID is 0, no more documents
        $!exhausted = True if $!cursor-id == 0;
    } else {
        die "GetMore failed: {$response<errmsg>}";
    }
}

method kill(--> Promise) {
    return start {
        return if $!cursor-id == 0;

        my %command = %(
            killCursors => $!collection,
            cursors => [$!cursor-id],
        );

        await $!connection.run-command(%command, $!database);
        $!cursor-id = 0;
        $!exhausted = True;
    };
}
