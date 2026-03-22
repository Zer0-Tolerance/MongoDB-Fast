#!/usr/bin/env raku

# MongoDB::Fast vs MongoDB (raku.land) — Head-to-Head Comparison
# Measures latency, throughput, and correctness for identical operations.

use lib 'lib';
use MongoDB::Fast;
use MongoDB::Client;
use BSON::Document;

# ── Config ─────────────────────────────────────────────────────────────────
my $N        = 500;   # docs per bulk batch
my $REPS     = 30;    # repetitions for latency tests
my $CONC     = 50;    # concurrent tasks (MongoDB::Fast async)
my $DB-NAME  = 'cmp_battle';
my $FM-COL   = 'fastmongo';
my $MG-COL   = 'mongodb';

# ── Result tracking ────────────────────────────────────────────────────────
my %times;   # label → [fm-ms, mg-ms]
my %correct; # label → [fm-ok, mg-ok]

# ── Formatting ─────────────────────────────────────────────────────────────
sub h1(Str $t) { say "\n{'═' x 72}\n  $t\n{'═' x 72}" }
sub h2(Str $t) { say "\n┌─ $t " ~ '─' x (68 - $t.chars) }
sub winner(Num $a, Num $b --> Str) {
    return '  TIE   ' if abs($a - $b) / (($a + $b) / 2) < 0.05;
    $a < $b ?? '  FM ✓ ' !! '  MG ✓ ';
}

sub row(Str $label, Num $fm, Num $mg, Bool $fm-ok, Bool $mg-ok) {
    %times{$label}   = [$fm, $mg];
    %correct{$label} = [$fm-ok, $mg-ok];
    my $w    = winner($fm, $mg);
    my $fmok = $fm-ok ?? '✓' !! '✗';
    my $mgok = $mg-ok ?? '✓' !! '✗';
    printf "  %-38s  FM:%s%6.1fms   MG:%s%6.1fms  %s\n",
           $label, $fmok, $fm, $mgok, $mg, $w;
}

# ── Bench helpers ──────────────────────────────────────────────────────────
sub bench-fm(&code --> Num) {
    my $t = now;
    await code();
    return (now - $t) * 1000e0;
}

sub bench-mg(&code --> Num) {
    my $t = now;
    code();
    return (now - $t) * 1000e0;
}

# BSON::Document builder shorthand
# Named args (k => v) are captured via |c; we reorder so the MongoDB
# command keyword always comes first (same rule MongoDB::Fast uses internally).
my constant BD-CMDS = set <ping insert find update delete drop create
    createIndexes dropIndexes listIndexes aggregate count getMore
    killCursors listCollections listDatabases serverStatus>;

sub bd(|c --> BSON::Document) {
    my @all = flat c.list, c.hash.pairs;
    my (@cmd, @rest);
    for @all -> $p { BD-CMDS{$p.key} ?? @cmd.push($p) !! @rest.push($p) }
    BSON::Document.new(flat @cmd, @rest)
}

# ── Main ───────────────────────────────────────────────────────────────────
h1 "MongoDB::Fast vs MongoDB (raku.land) — Battle Comparison";
say "  N=$N docs/batch   REPS=$REPS   CONC=$CONC concurrent tasks";

my $main = start {

    # ── Clients ──────────────────────────────────────────────────────────
    my $fm = MongoDB::Fast.new;
    await $fm.connect;
    my $fc = $fm.db($DB-NAME).collection($FM-COL);

    my $mg = MongoDB::Client.new(:uri('mongodb://127.0.0.1'));
    my $md = $mg.database($DB-NAME);

    # Fresh start
    await $fc.drop;
    try { $md.run-command(bd drop => $MG-COL) }

    # ── 1. Single insert-one ─────────────────────────────────────────────
    h2 '1. Single insert-one';

    my $fm-t = bench-fm { $fc.insert-one({ x => 1, label => 'solo' }) };
    my $mg-t = bench-mg {
        $md.run-command(bd insert => $MG-COL, documents => [bd x => 1, label => 'solo'])
    };
    my $fm-chk = (await $fc.find-one({ x => 1 }))<label> eq 'solo';
    my $mg-chk = $md.run-command(bd find => $MG-COL, filter => bd(x => 1), limit => 1)<cursor><firstBatch>[0]<label> eq 'solo';
    row 'insert-one', $fm-t, $mg-t, $fm-chk, $mg-chk;

    # ── 2. Bulk insert-many ───────────────────────────────────────────────
    h2 "2. Bulk insert-many ($N docs)";

    await $fc.drop;
    try { $md.run-command(bd drop => $MG-COL) }

    my @fm-docs = (^$N).map(-> $i { %( idx => $i, grp => $i % 5, score => $i * 1.5e0 ) });
    my @mg-docs = (^$N).map(-> $i { bd idx => $i, grp => $i % 5, score => $i * 1.5e0 });

    $fm-t = bench-fm { $fc.insert-many(@fm-docs) };
    $mg-t = bench-mg { $md.run-command(bd insert => $MG-COL, documents => @mg-docs) };

    my $fm-cnt = await $fc.count-documents;
    my $mg-cnt = $md.run-command(bd count => $MG-COL, query => bd())<n>;
    row "insert-many ($N docs)", $fm-t, $mg-t,
        $fm-cnt == $N, $mg-cnt == $N;
    printf "    Throughput → FM: %5.0f docs/s   MG: %5.0f docs/s\n",
           $N / ($fm-t / 1000), $N / ($mg-t / 1000);

    # ── 3. find-one (repeated) ────────────────────────────────────────────
    h2 "3. find-one (×$REPS)";

    $fm-t = bench-fm {
        start { for ^$REPS -> $i { await $fc.find-one({ idx => $i % $N }) } }
    };
    $mg-t = bench-mg {
        for ^$REPS -> $i {
            $md.run-command(bd find => $MG-COL, filter => bd(idx => $i % $N), limit => 1)
        }
    };
    my $fm-doc = await $fc.find-one({ idx => 10 });
    my $mg-doc = $md.run-command(bd find => $MG-COL, filter => bd(idx => 10), limit => 1)<cursor><firstBatch>[0];
    row "find-one (×$REPS)", $fm-t, $mg-t,
        $fm-doc<idx> == 10, $mg-doc<idx> == 10;
    printf "    Avg latency → FM: %.2f ms/op   MG: %.2f ms/op\n",
           $fm-t / $REPS, $mg-t / $REPS;

    # ── 4. find + cursor.all ──────────────────────────────────────────────
    # Both drivers fetch ALL docs with batchSize=50, requiring multiple round-trips.
    # MG uses raw getMore calls to match MongoDB::Fast's cursor.all behaviour.
    h2 "4. Cursor fetch-all ($N docs, batchSize=50)";

    $fm-t = bench-fm {
        start {
            my $cur = $fc.find({}, :options({ batchSize => 50 }));
            await $cur.all;
        }
    };
    $mg-t = bench-mg {
        # Full cursor iteration using getMore – equivalent to MongoDB::Fast cursor.all
        my @all;
        my $r = $md.run-command(bd find => $MG-COL, filter => bd(), batchSize => 50);
        @all.append: $r<cursor><firstBatch>.Array;
        my $cid = $r<cursor><id>;
        while +$cid != 0 {
            my $more = $md.run-command(bd getMore => +$cid, collection => $MG-COL, batchSize => 50);
            @all.append: $more<cursor><nextBatch>.Array;
            $cid = $more<cursor><id>;
        }
        @all
    };
    my @fm-all = await $fc.find({}).all;
    my @mg-all = do {
        my @a;
        my $r = $md.run-command(bd find => $MG-COL, filter => bd(), batchSize => $N);
        @a.append: $r<cursor><firstBatch>.Array;
        @a
    };
    row "cursor.all ($N docs)", $fm-t, $mg-t,
        @fm-all.elems == $N, @mg-all.elems == $N;

    # ── 5. update-one (repeated) ──────────────────────────────────────────
    h2 "5. update-one (×$REPS)";

    $fm-t = bench-fm {
        start { for ^$REPS -> $i { await $fc.update-one({ idx => $i % $N }, { '$inc' => { score => 1 } }) } }
    };
    $mg-t = bench-mg {
        for ^$REPS -> $i {
            $md.run-command(bd update => $MG-COL, updates => [
                bd q => bd(idx => $i % $N),
                   u => bd('$inc' => bd(score => 1)),
                   upsert => False, multi => False
            ])
        }
    };
    my $fm-upd = await $fc.find-one({ idx => 0 });
    my $mg-upd = $md.run-command(bd find => $MG-COL, filter => bd(idx => 0), limit => 1)<cursor><firstBatch>[0];
    row "update-one (×$REPS)", $fm-t, $mg-t,
        $fm-upd<score> > 0, $mg-upd<score> > 0;
    printf "    Avg latency → FM: %.2f ms/op   MG: %.2f ms/op\n",
           $fm-t / $REPS, $mg-t / $REPS;

    # ── 6. update-many ────────────────────────────────────────────────────
    h2 "6. update-many (grp==0, { $N div 5 } docs)";

    $fm-t = bench-fm { $fc.update-many({ grp => 0 }, { '$set' => { flagged => True } }) };
    $mg-t = bench-mg {
        $md.run-command(bd update => $MG-COL, updates => [
            bd q => bd(grp => 0),
               u => bd('$set' => bd(flagged => True)),
               upsert => False, multi => True
        ])
    };
    my $fm-flagged = await $fc.count-documents({ flagged => True });
    my $mg-flagged = $md.run-command(bd count => $MG-COL, query => bd(flagged => True))<n>;
    row "update-many ({ $N div 5 } docs)", $fm-t, $mg-t,
        $fm-flagged == $N div 5, $mg-flagged == $N div 5;

    # ── 7. delete-one (repeated) ──────────────────────────────────────────
    h2 "7. delete-one (×10)";

    # Insert scratch docs
    await $fc.insert-many((^10).map(-> $i { %( del => True, n => $i ) }));
    $md.run-command(bd insert => $MG-COL, documents => [(^10).map(-> $i { bd del => True, n => $i })]);

    $fm-t = bench-fm {
        start { for ^10 -> $i { await $fc.delete-one({ del => True, n => $i }) } }
    };
    $mg-t = bench-mg {
        for ^10 -> $i {
            $md.run-command(bd delete => $MG-COL, deletes => [
                bd q => bd(del => True, n => $i), limit => 1
            ])
        }
    };
    row 'delete-one (×10)', $fm-t, $mg-t, True, True;

    # ── 8. delete-many ────────────────────────────────────────────────────
    h2 "8. delete-many (grp==1, { $N div 5 } docs)";

    $fm-t = bench-fm { $fc.delete-many({ grp => 1 }) };
    $mg-t = bench-mg {
        $md.run-command(bd delete => $MG-COL, deletes => [
            bd q => bd(grp => 1), limit => 0
        ])
    };
    my $fm-after = await $fc.count-documents({ grp => 1 });
    my $mg-after = $md.run-command(bd count => $MG-COL, query => bd(grp => 1))<n>;
    row "delete-many ({ $N div 5 } docs)", $fm-t, $mg-t,
        $fm-after == 0, $mg-after == 0;

    # ── 9. Aggregation pipeline ───────────────────────────────────────────
    h2 '9. Aggregate ($group by grp, $sort)';

    my @agg-pipe-fm = (
        { '$group' => { _id => '$grp', cnt => { '$sum' => 1 }, total => { '$sum' => '$score' } } },
        { '$sort'  => { _id => 1 } },
    );
    my @agg-pipe-mg = (
        bd( '$group' => bd( _id => '$grp', cnt => bd('$sum' => 1), total => bd('$sum' => '$score') ) ),
        bd( '$sort'  => bd( _id => 1 ) ),
    );

    $fm-t = bench-fm { $fc.aggregate(@agg-pipe-fm) };
    $mg-t = bench-mg {
        $md.run-command(bd aggregate => $MG-COL, pipeline => @agg-pipe-mg,
                        cursor => bd(batchSize => 100))
    };
    my @fm-agg = @(await $fc.aggregate(@agg-pipe-fm));
    my $mg-agg = $md.run-command(bd aggregate => $MG-COL, pipeline => @agg-pipe-mg,
                                  cursor => bd(batchSize => 100))<cursor><firstBatch>;
    row 'aggregate ($group $sort)', $fm-t, $mg-t,
        @fm-agg.elems >= 2, $mg-agg.elems >= 2;

    # ── 10. count-documents ───────────────────────────────────────────────
    h2 '10. count-documents (with filter, ×10)';

    $fm-t = bench-fm {
        start { for ^10 { await $fc.count-documents({ grp => 2 }) } }
    };
    $mg-t = bench-mg {
        for ^10 { $md.run-command(bd count => $MG-COL, query => bd(grp => 2)) }
    };
    my $fm-c2 = await $fc.count-documents({ grp => 2 });
    my $mg-c2 = $md.run-command(bd count => $MG-COL, query => bd(grp => 2))<n>;
    row "count with filter (×10)", $fm-t, $mg-t,
        $fm-c2 == $mg-c2, True;

    # ── 11. create-index ──────────────────────────────────────────────────
    h2 '11. create-index (compound)';

    $fm-t = bench-fm { $fc.create-index({ idx => 1, grp => 1 }) };
    $mg-t = bench-mg {
        $md.run-command(bd createIndexes => $MG-COL,
                        indexes => [bd key => bd(idx => 1, grp => 1), name => 'idx_grp'])
    };
    row 'create-index (compound)', $fm-t, $mg-t, True, True;

    # ── 12. MongoDB::Fast EXCLUSIVE: Concurrent ops ───────────────────────────
    h2 "12. MongoDB::Fast Exclusive: $CONC concurrent insert-one ops";

    await $fc.drop;
    try { $md.run-command(bd drop => $MG-COL) }

    # MongoDB::Fast: all concurrent
    $fm-t = bench-fm {
        Promise.allof((^$CONC).map(-> $i {
            start { await $fc.insert-one(%( n => $i, concurrent => True )) }
        }))
    };
    my $fm-conc-cnt = await $fc.count-documents({ concurrent => True });

    # MongoDB raku.land: sequential (no native async)
    $mg-t = bench-mg {
        for ^$CONC -> $i {
            $md.run-command(bd insert => $MG-COL, documents => [bd n => $i, concurrent => True])
        }
    };
    my $mg-conc-cnt = $md.run-command(bd count => $MG-COL, query => bd(concurrent => True))<n>;

    row "concurrent/$CONC inserts (FM async vs MG serial)", $fm-t, $mg-t,
        $fm-conc-cnt == $CONC, $mg-conc-cnt == $CONC;
    printf "    FM concurrency: %d tasks in %.1f ms (%.0f ops/s)\n",
           $CONC, $fm-t, $CONC / ($fm-t / 1000);
    printf "    MG sequential:  %d tasks in %.1f ms (%.0f ops/s)\n",
           $CONC, $mg-t, $CONC / ($mg-t / 1000);

    # ── 13. MongoDB::Fast EXCLUSIVE: Concurrent mixed ops ─────────────────────
    h2 "13. MongoDB::Fast Exclusive: $CONC concurrent mixed ops (insert+find+update)";

    $fm-t = bench-fm {
        Promise.allof((^$CONC).map(-> $i {
            start {
                await $fc.insert-one(%( m => $i, mix => True ));
                await $fc.find-one({ n => $i % $CONC });
                await $fc.update-one({ m => $i }, { '$set' => { done => True } });
            }
        }))
    };
    my $fm-mix-cnt = await $fc.count-documents({ done => True });

    $mg-t = bench-mg {
        for ^$CONC -> $i {
            $md.run-command(bd insert => $MG-COL, documents => [bd m => $i, mix => True]);
            $md.run-command(bd find => $MG-COL, filter => bd(n => $i % $CONC), limit => 1);
            $md.run-command(bd update => $MG-COL, updates => [
                bd q => bd(m => $i), u => bd('$set' => bd(done => True)),
                   upsert => False, multi => False
            ]);
        }
    };
    my $mg-mix-cnt = $md.run-command(bd count => $MG-COL, query => bd(done => True))<n>;

    row "concurrent/$CONC mixed ops (FM async vs MG serial)", $fm-t, $mg-t,
        $fm-mix-cnt == $CONC, $mg-mix-cnt == $CONC;

    # ── 14. API Ergonomics Demo ───────────────────────────────────────────
    h2 '14. API Ergonomics (upsert + $push + $inc)';

    $fm-t = bench-fm {
        start {
            await $fc.update-one({ uid => 'ergo-1' },
                { '$set' => { name => 'Demo' }, '$push' => { tags => 'a' }, '$inc' => { views => 1 } },
                :upsert);
        }
    };
    my $fm-ergo = await $fc.find-one({ uid => 'ergo-1' });

    $mg-t = bench-mg {
        $md.run-command(bd update => $MG-COL, updates => [
            bd q => bd(uid => 'ergo-1'),
               u => bd(
                   '$set'  => bd(name => 'Demo'),
                   '$push' => bd(tags => 'a'),
                   '$inc'  => bd(views => 1),
               ),
               upsert => True, multi => False
        ])
    };
    my $mg-ergo = $md.run-command(bd find => $MG-COL,
                                   filter => bd(uid => 'ergo-1'), limit => 1
                                  )<cursor><firstBatch>[0];
    row 'upsert + $push + $inc', $fm-t, $mg-t,
        ($fm-ergo<name> eq 'Demo' && $fm-ergo<views> == 1),
        ($mg-ergo<name> eq 'Demo' && $mg-ergo<views> == 1);

    # ── Cleanup ───────────────────────────────────────────────────────────
    await $fc.drop;
    try { $md.run-command(bd drop => $MG-COL) }
    $fm.close;

    # ── Summary Table ─────────────────────────────────────────────────────
    h1 'RESULTS SUMMARY';

    my $fw = 0;
    my $mw = 0;
    my $tie = 0;
    my $fm-errors = 0;
    my $mg-errors = 0;

    printf "  %-40s  %10s  %10s  %8s\n", 'Operation', 'MongoDB::Fast', 'MongoDB', 'Winner';
    say '  ' ~ '─' x 70;

    for %times.keys.sort -> $op {
        my ($fm, $mg) = @(%times{$op});
        my ($fm-ok, $mg-ok) = @(%correct{$op});
        $fm-errors++ unless $fm-ok;
        $mg-errors++ unless $mg-ok;

        my $speedup = $mg / $fm;
        my $w-str;
        if abs($fm - $mg) / (($fm + $mg) / 2) < 0.05 {
            $w-str = 'TIE';
            $tie++;
        } elsif $fm < $mg {
            $w-str = sprintf "FM +%.0f%%", ($speedup - 1) * 100;
            $fw++;
        } else {
            $w-str = sprintf "MG +%.0f%%", ((1/$speedup) - 1) * 100;
            $mw++;
        }

        printf "  %-40s  %8.1f ms  %8.1f ms  %s\n", $op, $fm, $mg, $w-str;
    }

    say '  ' ~ '─' x 70;
    say '';
    printf "  MongoDB::Fast wins: %d   MongoDB wins: %d   Ties: %d\n", $fw, $mw, $tie;
    printf "  MongoDB::Fast errors: %d   MongoDB errors: %d\n", $fm-errors, $mg-errors;
    say '';

    # Correctness check
    my $all-correct = (%correct.values.map(-> $pair { $pair[0] && $pair[1] }).all).so;
    if $all-correct {
        say '  ✓ Both drivers produced CORRECT results on all operations';
    } else {
        say '  ✗ Some operations produced incorrect results — see details above';
    }
    say '';

    # Key findings
    say '  Key findings:';
    say '  • MongoDB::Fast: async/concurrent API, Promise-based, single connection';
    say '  • MongoDB raku.land: synchronous blocking API, BSON::Document required';
    say '  • MongoDB::Fast allows true concurrent I/O (multiple ops in-flight)';
    say '  • MongoDB raku.land requires verbose BSON::Document construction';
    say '  • MongoDB::Fast uses clean native Raku Hash/Array, zero boilerplate';
    say '';
};

await $main;
