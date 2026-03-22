use v6.d;

unit class MongoDB::Fast::ObjectID;

has Buf $.bytes;

method new(Buf $bytes) {
    die "ObjectID must be exactly 12 bytes" unless $bytes.elems == 12;
    self.bless(bytes => $bytes);
}

method Buf { $!bytes }
method gist { self.to-hex }
method Str { self.to-hex }

method to-hex {
    $!bytes.list.map(*.fmt('%02x')).join;
}
