use v6.d;
use NativeCall;
use MongoDB::Fast::ObjectID;

unit class MongoDB::Fast::BSON;

# MongoDB command keywords that must be ordered first in BSON documents
my constant CMD-KEYWORDS = set <ping insert find update delete drop create createIndexes dropIndexes listIndexes aggregate count getMore killCursors listCollections listDatabases serverStatus>;
my $lock=Lock::Async.new;
# BSON type codes
constant BSON_DOUBLE       = 0x01;
constant BSON_STRING       = 0x02;
constant BSON_DOCUMENT     = 0x03;
constant BSON_ARRAY        = 0x04;
constant BSON_BINARY       = 0x05;
constant BSON_UNDEFINED    = 0x06;
constant BSON_OBJECTID     = 0x07;
constant BSON_BOOLEAN      = 0x08;
constant BSON_DATETIME     = 0x09;
constant BSON_NULL         = 0x0A;
constant BSON_REGEX        = 0x0B;
constant BSON_JAVASCRIPT   = 0x0D;
constant BSON_INT32        = 0x10;
constant BSON_TIMESTAMP    = 0x11;
constant BSON_INT64        = 0x12;
constant BSON_DECIMAL128   = 0x13;
constant BSON_MINKEY       = 0xFF;
constant BSON_MAXKEY       = 0x7F;

# Fast BSON encoder using direct buffer manipulation
method encode(Hash $doc --> Buf) {
    my $buf = buf8.new;
    self!encode-document($doc, $buf);
    return $buf;
}

method !encode-document(Hash $doc, $buf) {
    my $start-pos = $buf.elems;

    # Reserve space for document size (4 bytes)
    $buf.append: 0, 0, 0, 0;

    # Single-scan key ordering: fast-path skips reordering for user data docs
    # (which never contain CMD-KEYWORDS or '$db')
    my @keys = $doc.keys;
    my $needs-ordering = False;
    my @cmd-first;
    my @db-key;
    my @other;
    for @keys -> $k {
        if CMD-KEYWORDS{$k}  { @cmd-first.push: $k; $needs-ordering = True }
        elsif $k eq '$db'    { @db-key.push: $k;    $needs-ordering = True }
        else                 { @other.push: $k }
    }

    if $needs-ordering {
        my @ordered-keys = @cmd-first && @db-key
            ?? flat(@cmd-first[0], @db-key[0], @cmd-first[1..*], @other)
            !! flat(@cmd-first, @db-key, @other);
        for @ordered-keys -> $key { self!encode-element($key, $doc{$key}, $buf) }
    } else {
        for @keys -> $key { self!encode-element($key, $doc{$key}, $buf) }
    }

    # Add terminating null byte
    $buf.append: 0;

    # Write document size (little-endian int32)
    my $size = $buf.elems - $start-pos;
    $buf[$start-pos]     = $size +& 0xFF;
    $buf[$start-pos + 1] = ($size +> 8) +& 0xFF;
    $buf[$start-pos + 2] = ($size +> 16) +& 0xFF;
    $buf[$start-pos + 3] = ($size +> 24) +& 0xFF;
}

# Dedicated array encoder: avoids intermediate Hash and CMD-KEYWORDS ordering
method !encode-array(@arr, $buf) {
    my $start-pos = $buf.elems;
    $buf.append: 0, 0, 0, 0;
    my $i = 0;
    for @arr -> $item {
        self!encode-element($i++.Str, $item, $buf);
    }
    $buf.append: 0;
    my $size = $buf.elems - $start-pos;
    $buf[$start-pos]     = $size +& 0xFF;
    $buf[$start-pos + 1] = ($size +> 8) +& 0xFF;
    $buf[$start-pos + 2] = ($size +> 16) +& 0xFF;
    $buf[$start-pos + 3] = ($size +> 24) +& 0xFF;
}

method !encode-element(Str $key, $value, $buf) {
    given $value {
        # Bool must come before Int because Bool is a subtype of Int in Raku
        when Bool {
            $buf.append: BSON_BOOLEAN;
            self!encode-cstring($key, $buf);
            $buf.append: $_ ?? 1 !! 0;
        }
        when Int {
            if -2147483648 <= $_ <= 2147483647 {
                # 32-bit integer
                $buf.append: BSON_INT32;
                self!encode-cstring($key, $buf);
                self!encode-int32($_, $buf);
            } else {
                # 64-bit integer
                $buf.append: BSON_INT64;
                self!encode-cstring($key, $buf);
                self!encode-int64($_, $buf);
            }
        }
        when Rat | Num {
            $buf.append: BSON_DOUBLE;
            self!encode-cstring($key, $buf);
            self!encode-double($_, $buf);
        }
        when Str {
            $buf.append: BSON_STRING;
            self!encode-cstring($key, $buf);
            self!encode-string($_, $buf);
        }
        when Hash {
            $buf.append: BSON_DOCUMENT;
            self!encode-cstring($key, $buf);
            self!encode-document($_, $buf);
        }
        when Array | Positional | Seq {
            $buf.append: BSON_ARRAY;
            self!encode-cstring($key, $buf);
            # Use .list to decontainerize (given/when itemizes $_)
            self!encode-array($_.list, $buf);
        }
        when MongoDB::Fast::ObjectID {
            $buf.append: BSON_OBJECTID;
            self!encode-cstring($key, $buf);
            $buf.append: .bytes;
        }
        when Buf {
            $buf.append: BSON_BINARY;
            self!encode-cstring($key, $buf);
            self!encode-int32(.elems, $buf);
            $buf.append: 0;  # Binary subtype: generic
            $buf.append: $_;
        }
        when Nil | Any:U {
            $buf.append: BSON_NULL;
            self!encode-cstring($key, $buf);
        }
        when DateTime {
            $buf.append: BSON_DATETIME;
            self!encode-cstring($key, $buf);
            my $millis = (.posix * 1000).Int;
            self!encode-int64($millis, $buf);
        }
        when Instant {
            $buf.append: BSON_DATETIME;
            self!encode-cstring($key, $buf);
            my $millis = (.Num * 1000).Int;
            self!encode-int64($millis, $buf);
        }
        when Pair {
            # If Pair looks like a key => value hash, encode as document
            # This handles the case where { x => 'y' } becomes a Pair in certain contexts
            $buf.append: BSON_DOCUMENT;
            self!encode-cstring($key, $buf);
            my %pair-as-hash = .key => .value;
            self!encode-document(%pair-as-hash, $buf);
        }
        default {
            die "Unsupported BSON type: {.^name}";
        }
    }
}

method !encode-cstring(Str $str, $buf) {
    $buf.append: $str.encode('UTF-8');
    $buf.append: 0;
}

method !encode-string(Str $str, $buf) {
    my $encoded = $str.encode('UTF-8');
    my $len = $encoded.elems + 1;  # +1 for null terminator
    self!encode-int32($len, $buf);
    $buf.append: $encoded;
    $buf.append: 0;
}

method !encode-int32(Int $val, $buf) {
    $buf.append: $val +& 0xFF;
    $buf.append: ($val +> 8) +& 0xFF;
    $buf.append: ($val +> 16) +& 0xFF;
    $buf.append: ($val +> 24) +& 0xFF;
}

method !encode-int64(Int $val, $buf) {
    for ^8 -> $i {
        $buf.append: ($val +> ($i * 8)) +& 0xFF;
    }
}

method !encode-double(Num() $val, $buf) {
    # Encode as IEEE 754 double (little-endian)
    my $num-buf = CArray[num64].new;
    $num-buf[0] = $val;

    my $int-buf = nativecast(CArray[uint64], $num-buf);
    my $bits = $int-buf[0];

    # Write as little-endian bytes
    for ^8 -> $i {
        $buf.append: ($bits +> ($i * 8)) +& 0xFF;
    }
}

# Fast BSON decoder
method decode(Buf $buf --> Hash) {
    my $pos = 0;
    return self!decode-document($buf, $pos);
}

# Decode at a given position, advancing $pos past the document (no subbuf copy)
method decode-at(Buf $buf, $pos is rw --> Hash) {
    return self!decode-document($buf, $pos);
}

# Encode directly into an existing buffer (no intermediate Buf allocation)
method encode-into(Hash $doc, $buf) {
    self!encode-document($doc, $buf);
}

method !decode-document(Buf $buf, $pos is rw --> Hash) {
    my $size = self!decode-int32($buf, $pos);
    my $end-pos = $pos - 4 + $size;

    my %doc;

    while $pos < $end-pos {
        my $type = $buf[$pos++];
        last if $type == 0;  # Document terminator

        my $key = self!decode-cstring($buf, $pos);
        my $value = self!decode-element($type, $buf, $pos);

        %doc{$key} = $value;
    }

    return %doc;
}

method !decode-element(Int $type, Buf $buf, $pos is rw) {
    given $type {
        when BSON_DOUBLE {
            return self!decode-double($buf, $pos);
        }
        when BSON_STRING {
            return self!decode-string($buf, $pos);
        }
        when BSON_DOCUMENT {
            return self!decode-document($buf, $pos);
        }
        when BSON_ARRAY {
            my %array-doc = self!decode-document($buf, $pos);
            return [%array-doc.sort(*.key.Int).map(*.value)];
        }
        when BSON_BINARY {
            my $len = self!decode-int32($buf, $pos);
            my $subtype = $buf[$pos++];
            my $data = $buf.subbuf($pos, $len);
            $pos += $len;
            return $data;
        }
        when BSON_BOOLEAN {
            return so $buf[$pos++];
        }
        when BSON_DATETIME {
            my $millis = self!decode-int64($buf, $pos);
            return DateTime.new($millis / 1000);
        }
        when BSON_NULL {
            return Any;
        }
        when BSON_INT32 {
            return self!decode-int32($buf, $pos);
        }
        when BSON_INT64 {
            return self!decode-int64($buf, $pos);
        }
        when BSON_OBJECTID {
            my $oid-buf = $buf.subbuf($pos, 12);
            $pos += 12;
            return MongoDB::Fast::ObjectID.new($oid-buf);
        }
        when BSON_TIMESTAMP {
            # MongoDB timestamp (64-bit value)
            my $timestamp = self!decode-int64($buf, $pos);
            return $timestamp;
        }
        default {
            die "Unsupported BSON type: 0x{$type.base(16)}";
        }
    }
}

method !decode-cstring(Buf $buf, $pos is rw --> Str) {
    my $start = $pos;
    while $buf[$pos] != 0 {
        $pos++;
    }
    my $str = $buf.subbuf($start, $pos - $start).decode('UTF-8');
    $pos++;  # Skip null terminator
    return $str;
}

method !decode-string(Buf $buf, $pos is rw --> Str) {
    my $len = self!decode-int32($buf, $pos);
    my $str = $buf.subbuf($pos, $len - 1).decode('UTF-8');
    $pos += $len;
    return $str;
}

method !decode-int32(Buf $buf, $pos is rw --> Int) {
    my $val = $buf[$pos] +
              ($buf[$pos + 1] +< 8) +
              ($buf[$pos + 2] +< 16) +
              ($buf[$pos + 3] +< 24);
    $pos += 4;
    # Convert to signed
    return $val > 0x7FFFFFFF ?? $val - 0x100000000 !! $val;
}

method !decode-int64(Buf $buf, $pos is rw --> Int) {
    my $val = 0;
    for ^8 -> $i {
        $val += $buf[$pos + $i] +< ($i * 8);
    }
    $pos += 8;
    # Convert to signed
    return $val > 0x7FFFFFFFFFFFFFFF ?? $val - 0x10000000000000000 !! $val;
}

method !decode-double(Buf $buf, $pos is rw --> Num) {
    # Decode IEEE 754 double (little-endian)
    # Read 8 bytes as little-endian uint64
    my $bits = 0;
    for ^8 -> $i {
        $bits += $buf[$pos + $i] +< ($i * 8);
    }
    $pos += 8;

    # Convert uint64 bits to double
    my $int-buf = CArray[uint64].new;
    $int-buf[0] = $bits;

    my $num-buf = nativecast(CArray[num64], $int-buf);
    return $num-buf[0];
}

# Utility: Generate ObjectID (optimized for bulk generation)
method generate-object-id(--> MongoDB::Fast::ObjectID) {
    state $counter = 0;
    # Cache random bytes - only need to generate once per process
    state $random-bytes = buf8.new((^256).pick(5));
    # Cache timestamp and timestamp bytes - update every second
    state $last-timestamp = 0;
    state $timestamp-bytes = buf8.new(0, 0, 0, 0);

    my $timestamp = now.Int;
    # $timestamp = now.Rat;

    # Only recompute timestamp bytes when second changes
    if $timestamp != $last-timestamp {
        $last-timestamp = $timestamp;
        $timestamp-bytes[0] = ($timestamp +> 24) +& 0xFF;
        $timestamp-bytes[1] = ($timestamp +> 16) +& 0xFF;
        $timestamp-bytes[2] = ($timestamp +> 8) +& 0xFF;
        $timestamp-bytes[3] = $timestamp +& 0xFF;
    }

    # Pre-allocate buffer with known size
    my $buf = buf8.allocate(12);

    # Timestamp (4 bytes, big-endian) - use cached bytes
    $buf[0] = $timestamp-bytes[0];
    $buf[1] = $timestamp-bytes[1];
    $buf[2] = $timestamp-bytes[2];
    $buf[3] = $timestamp-bytes[3];

    # Random value (5 bytes) - use cached random bytes
    $buf[4] = $random-bytes[0];
    $buf[5] = $random-bytes[1];
    $buf[6] = $random-bytes[2];
    $buf[7] = $random-bytes[3];
    $buf[8] = $random-bytes[4];

    # Counter (3 bytes, big-endian) - increment atomically
    # $counter⚛++;
    $lock.protect: {
    $counter = ($counter + 1) +& 0xFFFFFF;
    $buf[9] = ($counter +> 16) +& 0xFF;
    $buf[10] = ($counter +> 8) +& 0xFF;
    $buf[11] = $counter +& 0xFF;
    }

    return MongoDB::Fast::ObjectID.new($buf);
}

# Utility: ObjectID to hex string
method objectid-to-hex($oid --> Str) {
    given $oid {
        when MongoDB::Fast::ObjectID { return .to-hex }
        when Buf { return .list.map(*.fmt('%02x')).join }
        default { die "Expected ObjectID or Buf, got {.^name}" }
    }
}
