use v6.d;
use MongoDB::Fast::Wire;
use MongoDB::Fast::BSON;

unit class MongoDB::Fast::Connection;

has Str $.host = '127.0.0.1';
has Int $.port = 27017;
has Str $.username;
has Str $.password;
has Str $.auth-database = 'admin';
has IO::Socket::Async $.socket;
has MongoDB::Fast::Wire $.wire;
has MongoDB::Fast::BSON $.bson;
has Bool $.connected = False;
has Lock $.lock;
has Promise $.connect-promise;
# Async operation serializer: ensures only one request/response cycle runs at a time
has Promise $!op-serializer = Promise.kept(True);
# FIFO queue of vows — pre-registered synchronously so the reader never
# misses a response due to TCP coalescing.
has Channel $!vow-channel .= new;

# Reconnection configuration
has Bool $.enable-auto-reconnect = True;
has Int $.max-reconnect-attempts = 5;
has Num $.initial-retry-delay = 0.1e0;  # seconds
has Num $.max-retry-delay = 30e0;       # seconds
has Int $.connection-timeout = 10;      # seconds

# Reconnection state
has Int $!reconnect-attempts = 0;
has Instant $!last-connection-attempt;
has Str $!last-error;

submethod BUILD(
    Str :$!host = '127.0.0.1',
    Int :$!port = 27017,
    Str :$!username,
    Str :$!password,
    Str :$!auth-database = 'admin',
    Bool :$!enable-auto-reconnect = True,
    Int :$!max-reconnect-attempts = 5,
    Num :$!initial-retry-delay = 0.1e0,
    Num :$!max-retry-delay = 30e0,
    Int :$!connection-timeout = 10
) {
    $!wire = MongoDB::Fast::Wire.new;
    $!bson = MongoDB::Fast::BSON.new;
    $!lock = Lock.new;
    $!reconnect-attempts = 0;
}

method connect(--> Promise) {
    return Promise.kept(True) if $!connected;

    $!connect-promise //= start {
        my $sock = await IO::Socket::Async.connect($!host, $!port);
        $!socket = $sock;
        $!connected = True;

        # Authenticate if credentials provided
        if $!username {
            await self!authenticate;
        }

        # Start persistent background reader — one Supply tap for the lifetime
        # of this connection. Completed messages are routed via $!vow-channel.
        self!start-reader;

        True;
    };

    return $!connect-promise;
}

method !authenticate(--> Promise) {
    # SCRAM-SHA-256 authentication
    # NOTE: Authentication requires Digest::SHA256::Native or similar module
    # For now, authentication is disabled. Connect to MongoDB without auth
    # or configure MongoDB to allow connections without authentication.

    return start {
        warn "Authentication is currently not fully implemented.";
        warn "Please ensure your MongoDB instance allows connections without authentication,";
        warn "or install required authentication modules.";
        True;
    };

    # TODO: Implement full SCRAM-SHA-256 authentication
    # Requires: Digest::SHA256::Native or Digest::SHA
    # See MongoDB SCRAM-SHA-256 specification
}

# Disconnect and mark connection as closed
method !disconnect() {
    if $!socket {
        try {
            $!socket.close;
            CATCH {
                default { }
            }
        }
    }
    $!connected = False;
    $!socket = Nil;
    $!connect-promise = Nil;
}

# Calculate exponential backoff delay
method !calculate-retry-delay(--> Num) {
    my $delay = $!initial-retry-delay * (2 ** $!reconnect-attempts);
    return $delay min $!max-retry-delay;
}

# Reconnect with exponential backoff
method !reconnect(--> Promise) {
    start {
        if !$!enable-auto-reconnect {
            False;
        } elsif $!reconnect-attempts >= $!max-reconnect-attempts {
            False;
        } else {
            # Calculate and apply backoff delay
            my $delay = self!calculate-retry-delay;
            note "Connection lost. Attempting reconnect {$!reconnect-attempts + 1}/{$!max-reconnect-attempts} after {$delay.fmt('%.2f')}s...";

            await Promise.in($delay);

            # Close existing connection
            self!disconnect;

            # Increment attempt counter
            $!reconnect-attempts++;
            $!last-connection-attempt = now;

            # Try to reconnect with timeout
            my $connect-attempt = start {
                my $sock = await IO::Socket::Async.connect($!host, $!port);
                $!socket = $sock;
                $!connected = True;

                # Re-authenticate if credentials provided
                if $!username {
                    await self!authenticate;
                }

                # Restart the background reader for the new socket
                self!start-reader;

                True;
            };

            my $timeout = Promise.in($!connection-timeout);

            my $promise-result;
            my $catch-error;
            try {
                $promise-result = await Promise.anyof($connect-attempt, $timeout);
                CATCH {
                    default {
                        $catch-error = $_;
                    }
                }
            }
            if $catch-error {
                $!last-error = $catch-error.message // $catch-error;
                note "Reconnection attempt {$!reconnect-attempts} failed: {$!last-error}";
                return False;
            }

            if $timeout.status == Kept {
                $!last-error = "Connection timeout after {$!connection-timeout}s";
                note "Reconnection attempt {$!reconnect-attempts} failed: timeout";
                False;
            } elsif $connect-attempt.status == Broken {
                $!last-error = $connect-attempt.cause.gist;
                note "Reconnection attempt {$!reconnect-attempts} failed: {$!last-error}";
                False;
            } elsif $connect-attempt.status == Kept {
                # Success - reset counter
                note "Reconnection successful after {$!reconnect-attempts} attempt(s)";
                $!reconnect-attempts = 0;
                $!last-error = Nil;
                True;
            } else {
                $!last-error = "Unknown error during reconnection";
                note "Reconnection attempt {$!reconnect-attempts} failed: unknown error";
                False;
            }
        }
    }
}

# Health check - ping the server (does not use retry logic)
method ping(--> Promise) {
    self!execute-with-retry: -> {
        start {
            await self.connect unless $!connected;

            # Build and send ping command
            my $full-response = await self!send-recv(
                $!wire.build-op-msg({ ping => 1 }, 'admin'), :timeout(5));

            # Parse response
            my $parsed = $!wire.parse-op-msg($full-response);
            my $response = $parsed<documents>[0];

            so $response<ok> == 1;
        }
    }
}

# Check if connection is alive
method is-alive(--> Promise) {
    start {
        if !$!connected {
            False;
        } elsif !$!socket {
            False;
        } else {
            await self.ping;
        }
    }
}

# Persistent background reader — started once per connection in connect().
# Assembles complete BSON messages from the socket byte stream and delivers
# each finished message to the next vow in $!vow-channel (FIFO).
method !start-reader() {
    start {
        my $buf = buf8.new;
        react {
            whenever $!socket.Supply(:bin) -> $chunk {
                $buf.append: $chunk;
                # Deliver all complete messages that fit in the buffer
                loop {
                    last if $buf.elems < 4;
                    my $msg-len = $buf[0] + ($buf[1] +< 8)
                                          + ($buf[2] +< 16)
                                          + ($buf[3] +< 24);
                    last if $buf.elems < $msg-len;
                    my $msg = $buf.subbuf(0, $msg-len);
                    $buf   = $buf.subbuf($msg-len);
                    if my $vow = $!vow-channel.poll {
                        $vow.keep($msg);
                    }
                }
            }
            QUIT {
                # Socket error or closed — break all waiting operations
                while my $vow = $!vow-channel.poll {
                    $vow.break($_);
                }
                $!connected = False;
            }
        }
        CATCH {
            # Catch anything that escapes the react block (e.g. connection reset
            # on some MoarVM versions where QUIT doesn't fire) so the start{}
            # Promise never becomes an unhandled rejection and crashes the process.
            default {
                $!connected = False;
                while my $vow = $!vow-channel.poll {
                    $vow.break($_);
                }
            }
        }
    }
}

# Helper to execute command with automatic reconnection on failure
method !execute-with-retry(&operation --> Promise) {
    start {
        my $max-attempts = $!enable-auto-reconnect ?? $!max-reconnect-attempts + 1 !! 1;
        my $result;
        my $success = False;

        for ^$max-attempts -> $attempt {
            my $catch-error;
            try {
                $result = await &operation();
                # Success - reset reconnect counter
                $!reconnect-attempts = 0;
                $success = True;
                last;
                CATCH {
                    default {
                        $catch-error = $_;
                    }
                }
            }
            if $catch-error {
                my $err = $catch-error;
                my $is-connection-error = $err.message ~~ /:i 'connection' | 'socket' | 'broken' | 'closed' | 'timeout'/;
                if $is-connection-error && $!enable-auto-reconnect && $attempt < $max-attempts - 1 {
                    note "Operation failed with connection error: {$err.message // $err}";
                    self!disconnect;
                    my $reconnected = await self!reconnect;
                    if !$reconnected {
                        if $!reconnect-attempts >= $!max-reconnect-attempts {
                            die "Failed to reconnect after {$!max-reconnect-attempts} attempts. Last error: {$!last-error}";
                        }
                        # Will retry in next iteration
                    }
                } else {
                    # Not a connection error, or reconnection disabled, or final attempt
                    die $err;
                }
            }
        }

        if $success {
            $result;
        } else {
            die "Operation failed after {$max-attempts} attempts. Last error: {$!last-error}";
        }
    }
}

# Serialize socket send+receive.  Each call pre-registers a Promise vow in
# $!vow-channel synchronously (under $!lock), so the background reader always
# has the correct vow ready even when TCP coalesces multiple responses.
method !send-recv(Buf $msg, Int :$timeout = 30 --> Promise) {
    my ($p, $next);
    $!lock.protect: {
        $p = Promise.new;
        $!vow-channel.send($p.vow);   # pre-register before any async work
        my $prev = $!op-serializer;
        $next = $prev.then(-> $ {
            await $!socket.write($msg);
            await $p;
        });
        # Always resolve op-serializer so the next op can proceed regardless of outcome
        $!op-serializer = $next.then(-> $ { True });
    }
    $next;
}

method run-command(Hash $command, Str $database = 'admin', Int :$timeout = 30 --> Promise) {
    return self!execute-with-retry: -> {
        start {
            await self.connect unless $!connected;

            my $msg = $!wire.build-op-msg($command, $database);
            my $full-response = await self!send-recv($msg, :$timeout);
            my $parsed = $!wire.parse-op-msg($full-response);
            $parsed<documents>[0];
        }
    };
}

method run-command-with-docs(
    Hash $command,
    Str $identifier,
    @documents,
    Str $database = 'admin',
    Int :$timeout = 30
--> Promise) {
    return self!execute-with-retry: -> {
        start {
            await self.connect unless $!connected;

            my $msg = $!wire.build-op-msg-with-sequence(
                $command,
                $identifier,
                @documents,
                $database
            );

            my $full-response = await self!send-recv($msg, :$timeout);

            # Parse response
            my $parsed = $!wire.parse-op-msg($full-response);
            $parsed<documents>[0];
        }
    };
}

method close() {
    self!disconnect;
}

# Reset reconnection state (useful after manual intervention)
method reset-reconnection-state() {
    $!reconnect-attempts = 0;
    $!last-error = Nil;
}

# Get connection statistics
method connection-info(--> Hash) {
    return {
        host => $!host,
        port => $!port,
        connected => $!connected,
        auto-reconnect => $!enable-auto-reconnect,
        reconnect-attempts => $!reconnect-attempts,
        max-reconnect-attempts => $!max-reconnect-attempts,
        last-error => $!last-error,
        last-connection-attempt => $!last-connection-attempt,
    };
}

# Connection pool
class Pool {
    has Int $.max-connections = 10;
    has Int $.min-connections = 2;
    has @.connections;
    has @.available;
    has $.lock = Lock.new;
    has %.connection-params;

    submethod BUILD(
        Int :$!max-connections = 10,
        Int :$!min-connections = 2,
        :%!connection-params
    ) {
        # Initialize minimum connections
        for ^$!min-connections {
            my $conn = MongoDB::Fast::Connection.new(|%!connection-params);
            @!connections.push: $conn;
            @!available.push: $conn;
        }
    }

    method acquire(--> Promise) {
        return start {
            my $conn;

            $!lock.protect: {
                if @!available {
                    $conn = @!available.pop;
                } elsif @!connections.elems < $!max-connections {
                    $conn = MongoDB::Fast::Connection.new(|%!connection-params);
                    @!connections.push: $conn;
                }
            };

            # Wait if no connections available
            while !$conn {
                sleep 0.01;
                $!lock.protect: {
                    $conn = @!available.pop if @!available;
                };
            }

            await $conn.connect;
            $conn;
        };
    }

    method release(MongoDB::Fast::Connection $conn) {
        $!lock.protect: {
            @!available.push: $conn unless @!available.first($conn);
        };
    }

    method close-all() {
        for @!connections -> $conn {
            $conn.close;
        }
        @!connections = ();
        @!available = ();
    }
}
