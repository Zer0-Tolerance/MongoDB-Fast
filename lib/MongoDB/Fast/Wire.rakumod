use v6.d;
use MongoDB::Fast::BSON;

unit class MongoDB::Fast::Wire;

# Wire protocol opcodes
constant OP_REPLY        = 1;
constant OP_UPDATE       = 2001;
constant OP_INSERT       = 2002;
constant OP_QUERY        = 2004;
constant OP_GET_MORE     = 2005;
constant OP_DELETE       = 2006;
constant OP_KILL_CURSORS = 2007;
constant OP_MSG          = 2013;

# OP_MSG flags
constant FLAG_CHECKSUM_PRESENT = 1;
constant FLAG_MORE_TO_COME     = 2;
constant FLAG_EXHAUST_ALLOWED  = 1 +< 16;

has Int $.request-id = 0;
has MongoDB::Fast::BSON $.bson;

submethod BUILD() {
    $!bson = MongoDB::Fast::BSON.new;
}

method next-request-id(--> Int) {
    return ++$!request-id;
}

# Build OP_MSG message (modern MongoDB wire protocol)
method build-op-msg(Hash $command, Str $database = 'admin', Bool :$more-to-come = False --> Buf) {
    my $request-id = self.next-request-id;
    my $buf = buf8.new;
    my $flags = $more-to-come ?? FLAG_MORE_TO_COME !! 0;

    # Header with placeholder for messageLength (fixed up at end)
    $buf.append: 0, 0, 0, 0;                        # messageLength placeholder
    self!write-int32($buf, $request-id);
    self!write-int32($buf, 0);                       # responseTo
    self!write-int32($buf, OP_MSG);
    self!write-int32($buf, $flags);

    # Section 0 - Body: encode command directly into buf (no intermediate Buf)
    $buf.append: 0;                                  # kind
    my %cmd = %$command;
    %cmd{'$db'} = $database unless %cmd{'$db'}:exists;
    $!bson.encode-into(%cmd, $buf);

    # Fix up messageLength
    my $msg-size = $buf.elems;
    $buf[0] = $msg-size +& 0xFF;
    $buf[1] = ($msg-size +> 8) +& 0xFF;
    $buf[2] = ($msg-size +> 16) +& 0xFF;
    $buf[3] = ($msg-size +> 24) +& 0xFF;

    return $buf;
}

# Build OP_MSG with document sequence (Kind 1)
method build-op-msg-with-sequence(
    Hash $command,
    Str $identifier,
    @documents,
    Str $database = 'admin'
--> Buf) {
    my $request-id = self.next-request-id;
    my $buf = buf8.new;

    # Header with placeholder for messageLength (fixed up at end)
    $buf.append: 0, 0, 0, 0;                        # messageLength placeholder
    self!write-int32($buf, $request-id);
    self!write-int32($buf, 0);                       # responseTo
    self!write-int32($buf, OP_MSG);
    self!write-int32($buf, 0);                       # flags

    # Section 0 - Command: encode directly into buf
    $buf.append: 0;                                  # kind
    my %cmd = %$command;
    %cmd<$db> = $database;
    $!bson.encode-into(%cmd, $buf);

    # Section 1 - Document sequence with placeholder for section size
    $buf.append: 1;                                  # kind
    my $section1-size-pos = $buf.elems;
    $buf.append: 0, 0, 0, 0;                        # section size placeholder
    my $identifier-bytes = $identifier.encode('UTF-8');
    $buf.append: $identifier-bytes;
    $buf.append: 0;                                  # null terminator

    # Encode all documents directly into buf
    for @documents -> $doc {
        $!bson.encode-into($doc, $buf);
    }

    # Fix up section1-size (counts from start of size field itself)
    my $section1-size = $buf.elems - $section1-size-pos;
    $buf[$section1-size-pos]     = $section1-size +& 0xFF;
    $buf[$section1-size-pos + 1] = ($section1-size +> 8) +& 0xFF;
    $buf[$section1-size-pos + 2] = ($section1-size +> 16) +& 0xFF;
    $buf[$section1-size-pos + 3] = ($section1-size +> 24) +& 0xFF;

    # Fix up messageLength
    my $msg-size = $buf.elems;
    $buf[0] = $msg-size +& 0xFF;
    $buf[1] = ($msg-size +> 8) +& 0xFF;
    $buf[2] = ($msg-size +> 16) +& 0xFF;
    $buf[3] = ($msg-size +> 24) +& 0xFF;

    return $buf;
}

# Parse OP_MSG response
method parse-op-msg(Buf $response --> Hash) {
    my $pos = 0;

    # Read header
    my $msg-length = self!read-int32($response, $pos);
    my $request-id = self!read-int32($response, $pos);
    my $response-to = self!read-int32($response, $pos);
    my $opcode = self!read-int32($response, $pos);

    die "Expected OP_MSG (2013), got $opcode" unless $opcode == OP_MSG;

    # Read flags
    my $flags = self!read-int32($response, $pos);

    my @documents;

    # Read sections — decode-at advances $pos without copying the buffer
    while $pos < $msg-length {
        my $kind = $response[$pos++];

        given $kind {
            when 0 {
                # Body - single BSON document
                @documents.push: $!bson.decode-at($response, $pos);
            }
            when 1 {
                # Document sequence
                my $section-size = self!read-int32($response, $pos);
                my $section-end = $pos + $section-size - 4;

                # Skip identifier (cstring)
                while $response[$pos++] != 0 { }

                # Decode documents directly from response buffer
                while $pos < $section-end {
                    @documents.push: $!bson.decode-at($response, $pos);
                }
            }
            default {
                die "Unknown section kind: $kind";
            }
        }
    }

    return {
        requestID => $request-id,
        responseTo => $response-to,
        flags => $flags,
        documents => @documents,
    };
}

# Helper methods
method !write-int32($buf, Int $val) {
    $buf.append: $val +& 0xFF;
    $buf.append: ($val +> 8) +& 0xFF;
    $buf.append: ($val +> 16) +& 0xFF;
    $buf.append: ($val +> 24) +& 0xFF;
}

method !read-int32(Buf $buf, $pos is rw --> Int) {
    my $val = $buf[$pos] +
              ($buf[$pos + 1] +< 8) +
              ($buf[$pos + 2] +< 16) +
              ($buf[$pos + 3] +< 24);
    $pos += 4;
    return $val > 0x7FFFFFFF ?? $val - 0x100000000 !! $val;
}

method !read-cstring(Buf $buf, $pos is rw --> Str) {
    my $start = $pos;
    while $buf[$pos] != 0 {
        $pos++;
    }
    my $str = $buf.subbuf($start, $pos - $start).decode('UTF-8');
    $pos++;
    return $str;
}

# Legacy: Build OP_QUERY (for compatibility)
method build-op-query(
    Str $full-collection-name,
    Hash $query,
    Hash :$fields,
    Int :$skip = 0,
    Int :$limit = 0,
    Int :$flags = 0
--> Buf) {
    my $request-id = self.next-request-id;
    my $buf = buf8.new;

    my $query-bson = $!bson.encode($query);
    my $fields-bson = $fields ?? $!bson.encode($fields) !! Buf.new;

    my $msg-size = 16 +              # header
                   4 +               # flags
                   $full-collection-name.encode('UTF-8').elems + 1 +
                   4 +               # numberToSkip
                   4 +               # numberToReturn
                   $query-bson.elems +
                   $fields-bson.elems;

    # Header
    self!write-int32($buf, $msg-size);
    self!write-int32($buf, $request-id);
    self!write-int32($buf, 0);
    self!write-int32($buf, OP_QUERY);

    # Query specifics
    self!write-int32($buf, $flags);
    $buf.append: $full-collection-name.encode('UTF-8');
    $buf.append: 0;  # null terminator
    self!write-int32($buf, $skip);
    self!write-int32($buf, $limit);
    $buf.append: $query-bson;
    $buf.append: $fields-bson if $fields-bson;

    return $buf;
}

# Parse OP_REPLY
method parse-op-reply(Buf $response --> Hash) {
    my $pos = 0;

    # Header
    my $msg-length = self!read-int32($response, $pos);
    my $request-id = self!read-int32($response, $pos);
    my $response-to = self!read-int32($response, $pos);
    my $opcode = self!read-int32($response, $pos);

    die "Expected OP_REPLY (1), got $opcode" unless $opcode == OP_REPLY;

    # Reply specifics
    my $flags = self!read-int32($response, $pos);
    my $cursor-id = self!read-int64($response, $pos);
    my $starting-from = self!read-int32($response, $pos);
    my $number-returned = self!read-int32($response, $pos);

    # Parse documents
    my @documents;
    for ^$number-returned {
        my $doc = $!bson.decode($response.subbuf($pos));
        @documents.push: $doc;
        my $doc-size = self!read-int32($response, $pos - 4);
        $pos += $doc-size - 4;
    }

    return {
        requestID => $request-id,
        responseTo => $response-to,
        flags => $flags,
        cursorID => $cursor-id,
        startingFrom => $starting-from,
        numberReturned => $number-returned,
        documents => @documents,
    };
}

method !read-int64(Buf $buf, $pos is rw --> Int) {
    my $val = 0;
    for ^8 -> $i {
        $val += $buf[$pos + $i] +< ($i * 8);
    }
    $pos += 8;
    return $val;
}
