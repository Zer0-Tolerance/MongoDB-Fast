#!/usr/bin/env raku

# MongoDB::Fast Battle Test
# Stress tests + edge case coverage for the MongoDB::Fast driver

use lib 'lib';
use MongoDB::Fast;

# ── Globals ────────────────────────────────────────────────────────────────
my $DB       = 'battle_test';
my $COL      = 'battle';
my int $PASS = 0;
my int $FAIL = 0;
my @FAILURES;

# ── Helpers ────────────────────────────────────────────────────────────────
sub pass(Str $label) {
    $PASS++;
    printf "  ✓  %-60s\n", $label;
}

sub fail(Str $label, $err = '') {
    $FAIL++;
    @FAILURES.push("$label: $err");
    printf "  ✗  %-60s  ← %s\n", $label, $err.Str.lines[0] // '?';
}

sub section(Str $title) {
    say '';
    say "┌─ $title " ~ '─' x (70 - $title.chars);
}

sub timed(Str $label, &code) {
    my $t0 = now;
    my $result;
    try {
        $result = code();
        printf "  ✓  %-55s %7.1f ms\n", $label, (now - $t0) * 1000;
        $PASS++;
        CATCH { default {
            $FAIL++;
            @FAILURES.push("$label: $_");
            printf "  ✗  %-55s  ← %s\n", $label, .Str.lines[0] // '?';
        }}
    }
    $result;
}

sub assert(Bool $cond, Str $label, Str $detail = '') {
    if $cond { pass $label }
    else      { fail $label, $detail }
}

# ── Main ───────────────────────────────────────────────────────────────────
my $main = start {

    say '=' x 72;
    say 'MongoDB::Fast Battle Test';
    say '=' x 72;

    my $client = MongoDB::Fast.new;
    await $client.connect;
    my $col = $client.db($DB).collection($COL);
    await $col.drop;

    # ── 1. Basic CRUD ──────────────────────────────────────────────────────
    section 'Basic CRUD';

    my $ins = await timed 'insert-one (simple doc)', { $col.insert-one({ x => 1, y => 'hello' }) };
    assert $ins<acknowledged>,        'insert-one acknowledged';
    assert $ins<insertedId>.defined,  'insert-one has insertedId';

    my $doc = await timed 'find-one (exact match)', { $col.find-one({ x => 1 }) };
    assert $doc.defined,          'find-one returned doc';
    assert $doc<y> eq 'hello',    'find-one field value correct';

    my $upd = await timed 'update-one ($set)', {
        $col.update-one({ x => 1 }, { '$set' => { y => 'world', z => 42 } })
    };
    assert $upd<matchedCount>  == 1, 'update-one matched 1';
    assert $upd<modifiedCount> == 1, 'update-one modified 1';

    my $updated = await $col.find-one({ x => 1 });
    assert $updated<y> eq 'world', 'update-one changed field';
    assert $updated<z> == 42,      'update-one added field';

    my $del = await timed 'delete-one', { $col.delete-one({ x => 1 }) };
    assert $del<deletedCount> == 1, 'delete-one removed 1 doc';

    my $cnt = await $col.count-documents;
    assert $cnt == 0, 'collection empty after delete';

    # ── 2. Data Type Edge Cases ────────────────────────────────────────────
    section 'Data Type Edge Cases';

    # Large positive integer
    await timed 'insert large int (2^31-1)', { $col.insert-one({ type => 'int', val => 2_147_483_647 }) };
    my $intdoc = await $col.find-one({ type => 'int' });
    assert $intdoc<val> == 2_147_483_647, 'large int round-trips correctly';

    # Float
    await timed 'insert float (pi)', { $col.insert-one({ type => 'float', val => 3.14159265358979 }) };
    my $fdoc = await $col.find-one({ type => 'float' });
    assert abs($fdoc<val> - 3.14159265358979) < 1e-9, 'float round-trips correctly';

    # Negative integer
    await timed 'insert negative int', { $col.insert-one({ type => 'neg', val => -99_999 }) };
    my $ndoc = await $col.find-one({ type => 'neg' });
    assert $ndoc<val> == -99_999, 'negative int round-trips';

    # Boolean
    await timed 'insert boolean true/false', {
        $col.insert-one({ type => 'bool', t => True, f => False })
    };
    my $bdoc = await $col.find-one({ type => 'bool' });
    assert $bdoc<t> == True,  'boolean True round-trips';
    assert $bdoc<f> == False, 'boolean False round-trips';

    # Unicode string
    my $unicode = "日本語 한국어 العربية 🚀🎯💡";
    await timed 'insert unicode string', { $col.insert-one({ type => 'unicode', s => $unicode }) };
    my $udoc = await $col.find-one({ type => 'unicode' });
    assert $udoc<s> eq $unicode, 'unicode string round-trips correctly';

    # Empty string
    await timed 'insert empty string', { $col.insert-one({ type => 'empty-str', s => '' }) };
    my $esdoc = await $col.find-one({ type => 'empty-str' });
    assert ($esdoc<s>.defined && $esdoc<s> eq ''), 'empty string round-trips';

    # Deeply nested document (5 levels)
    await timed 'insert deeply nested doc (5 levels)', {
        $col.insert-one({ type => 'nested', a => { b => { c => { d => { e => 'deep' } } } } })
    };
    my $nestedoc = await $col.find-one({ type => 'nested' });
    assert $nestedoc<a><b><c><d><e> eq 'deep', 'deeply nested field readable';

    # Large array (500 elements) — use %() to force Hash construction inside map
    my @big-arr = (^500).map(-> $i { %( idx => $i, val => "item-$i" ) });
    await timed 'insert doc with 500-element array', { $col.insert-one({ type => 'bigarray', arr => @big-arr }) };
    my $arrdoc = await $col.find-one({ type => 'bigarray' });
    assert $arrdoc<arr>.elems == 500,      "large array has correct length (got {$arrdoc<arr>.elems})";
    assert $arrdoc<arr>[499]<idx> == 499,  "last array element correct (got {$arrdoc<arr>[499].raku})";

    # Empty array
    await timed 'insert doc with empty array', { $col.insert-one({ type => 'emptyarr', arr => [] }) };
    my $earrdoc = await $col.find-one({ type => 'emptyarr' });
    assert $earrdoc<arr>.elems == 0, 'empty array round-trips';

    # Large string (64 KB)
    my $big-str = 'X' x 65_536;
    await timed 'insert 64 KB string field', { $col.insert-one({ type => 'bigstr', s => $big-str }) };
    my $bsdoc = await $col.find-one({ type => 'bigstr' });
    assert $bsdoc<s>.chars == 65_536, '64KB string round-trips at correct length';

    # String with escape sequences / special chars
    my $special = "tab:\there\nnewline\"quote\\backslash";
    await timed 'insert string with special chars', { $col.insert-one({ type => 'special', s => $special }) };
    my $spdoc = await $col.find-one({ type => 'special' });
    assert $spdoc<s> eq $special, 'special-char string round-trips correctly';

    # Zero / falsy values
    await timed 'insert zero int and false bool', { $col.insert-one({ type => 'zeros', n => 0, b => False }) };
    my $zdoc = await $col.find-one({ type => 'zeros' });
    assert $zdoc<n> == 0,     'zero integer round-trips';
    assert $zdoc<b> == False, 'false boolean round-trips';

    await $col.drop;

    # ── 3. Bulk Operations ─────────────────────────────────────────────────
    section 'Bulk Operations';

    my @bulk = (^500).map(-> $i { { idx => $i, group => $i % 5, score => $i * 2.5 } });
    my $manyres = await timed 'insert-many (500 docs)', { $col.insert-many(@bulk) };
    assert $manyres<insertedCount> == 500,      'insert-many insertedCount == 500';
    assert $manyres<insertedIds>.elems == 500,  'insert-many has 500 insertedIds';

    my $total = await $col.count-documents;
    assert $total == 500, "count-documents == 500 (got $total)";

    my $filtered-cnt = await $col.count-documents({ group => 0 });
    assert $filtered-cnt == 100, "count-documents with filter == 100 (got $filtered-cnt)";

    my $upd-many = await timed 'update-many ($inc score for group 0)', {
        $col.update-many({ group => 0 }, { '$inc' => { score => 1000 } })
    };
    assert $upd-many<modifiedCount> == 100,
        "update-many modified 100 (got {$upd-many<modifiedCount>})";

    my $del-many = await timed 'delete-many (group == 4)', {
        $col.delete-many({ group => 4 })
    };
    assert $del-many<deletedCount> == 100,
        "delete-many removed 100 (got {$del-many<deletedCount>})";

    my $after = await $col.count-documents;
    assert $after == 400, "count after delete-many == 400 (got $after)";

    # ── 4. Query Operators ─────────────────────────────────────────────────
    section 'Query Operators';

    # $gt
    my $cursor-gt = $col.find({ idx => { '$gt' => 490 } });
    my @gt-docs = await $cursor-gt.all;
    assert @gt-docs.elems > 0, "\$gt query returns results ({@gt-docs.elems} docs)";

    # $in
    my $cursor-in = $col.find({ idx => { '$in' => [0, 10, 20, 30, 40] } });
    my @in-docs = await $cursor-in.all;
    assert @in-docs.elems == 5, "\$in query returns 5 docs (got {@in-docs.elems})";

    # $or
    my $cursor-or = $col.find({ '$or' => [ { idx => 1 }, { idx => 3 } ] });
    my @or-docs = await $cursor-or.all;
    assert @or-docs.elems == 2, "\$or query returns 2 docs (got {@or-docs.elems})";

    # $exists
    my $cursor-ex = $col.find({ idx => { '$exists' => True } });
    my @ex-docs = await $cursor-ex.all;
    assert @ex-docs.elems == 400, "\$exists query returns 400 docs (got {@ex-docs.elems})";

    # $ne
    my $cursor-ne = $col.find({ group => { '$ne' => 0 } });
    my @ne-docs = await $cursor-ne.all;
    assert @ne-docs.elems == 300, "\$ne query returns 300 docs (got {@ne-docs.elems})";

    # ── 5. Sort / Skip / Limit / Projection ───────────────────────────────
    section 'Sort / Skip / Limit / Projection';

    my $cursor-sorted = $col.find({}, :options({ sort => { idx => -1 }, limit => 5 }));
    my @sorted = await $cursor-sorted.all;
    assert @sorted.elems == 5, 'sort+limit 5 returns 5 docs';
    assert @sorted[0]<idx> > @sorted[4]<idx>, 'sort descending works';

    my $cursor-skip = $col.find({}, :options({ sort => { idx => 1 }, skip => 10, limit => 5 }));
    my @skipped = await $cursor-skip.all;
    assert @skipped.elems == 5, 'skip+limit returns 5 docs';
    # After deleting group=4 docs (idx 4,9,14,...), the 11th remaining doc (0-indexed skip=10) is idx=12
    assert @skipped[0]<idx> == 12, "skip 10 in modified collection lands at idx=12 (got {@skipped[0]<idx>})";

    my $cursor-proj = $col.find({ idx => 0 }, :options({ projection => { idx => 1, group => 1 } }));
    my @proj-docs = await $cursor-proj.all;
    assert @proj-docs.elems > 0, 'projection query returned docs';
    assert  @proj-docs[0]<idx>.defined,   'projection includes idx';
    assert  @proj-docs[0]<group>.defined, 'projection includes group';
    assert !@proj-docs[0]<score>.defined, 'projection excludes score';

    # ── 6. Aggregation Pipeline ────────────────────────────────────────────
    section 'Aggregation Pipeline';

    my @pipeline = (
        { '$match'  => { group => { '$lt' => 4 } } },
        { '$group'  => { _id => '$group', total => { '$sum' => '$score' }, count => { '$sum' => 1 } } },
        { '$sort'   => { _id => 1 } },
    );
    my @agg = @(await timed 'aggregate ($match $group $sort)', { $col.aggregate(@pipeline) });
    assert @agg.elems == 4, "aggregate returns 4 groups (got {@agg.elems})";
    assert @agg[0]<count> == 100, "each group has 100 docs (got {@agg[0]<count>})";

    my @agg-limited = @(await $col.aggregate([
        { '$match' => { group => 0 } },
        { '$limit' => 3 },
    ]));
    assert @agg-limited.elems == 3, 'aggregate $limit 3 returns 3 docs';

    my @agg-project = @(await $col.aggregate([
        { '$project' => { idx => 1, doubled => { '$multiply' => ['$idx', 2] } } },
        { '$limit' => 1 },
    ]));
    assert @agg-project[0]<doubled>.defined, 'aggregate $project computed field works';

    # ── 7. Upsert Behavior ─────────────────────────────────────────────────
    section 'Upsert Behavior';

    my $ups1 = await timed 'upsert (non-existent doc)', {
        $col.update-one({ uid => 'ghost-999' }, { '$set' => { name => 'Ghost', created => True } }, :upsert)
    };
    assert $ups1<upsertedId>.defined, 'upsert creates new doc and returns upsertedId';

    my $upserted-doc = await $col.find-one({ uid => 'ghost-999' });
    assert $upserted-doc<name> eq 'Ghost', 'upserted doc has correct fields';

    my $ups2 = await timed 'upsert (existing doc)', {
        $col.update-one({ uid => 'ghost-999' }, { '$set' => { name => 'Updated Ghost' } }, :upsert)
    };
    assert ($ups2<upsertedId> ~~ Nil || !$ups2<upsertedId>.defined),
        'upsert on existing doc returns no upsertedId';
    assert $ups2<modifiedCount> == 1, 'upsert on existing doc modifies 1';

    # ── 8. Replace One ─────────────────────────────────────────────────────
    section 'Replace One';

    await $col.insert-one({ rid => 1, keep => 'yes', extra => 'data' });
    my $rep = await timed 'replace-one (strips extra fields)', {
        $col.replace-one({ rid => 1 }, { rid => 1, keep => 'yes' })
    };
    assert $rep<modifiedCount> == 1, 'replace-one modifiedCount == 1';
    my $replaced = await $col.find-one({ rid => 1 });
    assert !$replaced<extra>.defined, 'replace-one removed non-specified field';
    assert  $replaced<keep> eq 'yes', 'replace-one kept specified field';

    # ── 9. Update Operators ────────────────────────────────────────────────
    section 'Update Operators';

    await $col.insert-one({ optest => True, counter => 0, tags => ['a', 'b'] });

    # $inc positive
    await $col.update-one({ optest => True }, { '$inc' => { counter => 5 } });
    my $d1 = await $col.find-one({ optest => True });
    assert $d1<counter> == 5, "\$inc: 0 + 5 == 5 (got {$d1<counter>})";

    # $inc negative (accumulate)
    await $col.update-one({ optest => True }, { '$inc' => { counter => -3 } });
    my $d2 = await $col.find-one({ optest => True });
    assert $d2<counter> == 2, "\$inc: 5 + (-3) == 2 (got {$d2<counter>})";

    # $set on existing field
    await $col.update-one({ optest => True }, { '$set' => { counter => 99 } });
    my $d3 = await $col.find-one({ optest => True });
    assert $d3<counter> == 99, "\$set overwrites existing field";

    # $unset removes field
    await $col.update-one({ optest => True }, { '$unset' => { counter => '' } });
    my $d4 = await $col.find-one({ optest => True });
    assert !$d4<counter>.defined, "\$unset removes field";

    # $push adds array element
    await $col.update-one({ optest => True }, { '$push' => { tags => 'c' } });
    my $d5 = await $col.find-one({ optest => True });
    assert ($d5<tags>.grep('c').elems > 0), "\$push adds element to array";
    assert $d5<tags>.elems == 3, "\$push: array now has 3 elements";

    # ── 10. Index Operations ───────────────────────────────────────────────
    section 'Index Operations';

    my $idx1 = await timed 'create-index (single field asc)', {
        $col.create-index({ idx => 1 })
    };
    assert $idx1, 'single-field index created';

    my $idx2 = await timed 'create-index (compound)', {
        $col.create-index({ group => 1, score => -1 }, :options({ name => 'group_score_idx' }))
    };
    assert $idx2, 'compound index created';

    # ── 11. Cursor Batch Iteration ─────────────────────────────────────────
    section 'Cursor Batch Iteration';

    # cursor.all() — 400 docs across multiple batches
    my $full-cursor = $col.find({}, :options({ batchSize => 50 }));
    my @all-docs = await timed 'cursor.all (400+ docs, batchSize=50)', { $full-cursor.all };
    assert @all-docs.elems >= 400, "cursor.all fetched >= 400 docs (got {@all-docs.elems})";

    # cursor.next()-by-next() iteration
    await $col.drop;
    await $col.insert-many((^10).map({ { seq => $_ } }));
    my $nc = $col.find({}, :options({ sort => { seq => 1 }, batchSize => 3 }));
    my @seq;
    loop {
        my $d = await $nc.next;
        last unless $d.defined;
        @seq.push($d<seq>);
    }
    assert @seq.elems == 10, "cursor next()-by-next() fetched all 10 (got {@seq.elems})";
    assert (@seq Z== (^10)).all.so, 'cursor next() order is correct';

    # ── 12. Concurrent Stress Test ─────────────────────────────────────────
    section 'Concurrent Stress Test';

    await $col.drop;
    my $CONC = 50;

    # 50 concurrent inserts
    await timed "$CONC concurrent insert-one ops", {
        Promise.allof((^$CONC).map(-> $i {
            start { await $col.insert-one({ stress => True, n => $i, ts => now.Num }) }
        }))
    };
    my $stress-cnt = await $col.count-documents({ stress => True });
    assert $stress-cnt == $CONC, "all $CONC concurrent inserts landed (got $stress-cnt)";

    # 50 concurrent find-one ops
    await timed "$CONC concurrent find-one ops", {
        Promise.allof((^$CONC).map(-> $i {
            start { await $col.find-one({ n => $i }) }
        }))
    };
    pass "$CONC concurrent find-ones completed without error";

    # 50 concurrent update-one ops
    await timed "$CONC concurrent update-one ops", {
        Promise.allof((^$CONC).map(-> $i {
            start { await $col.update-one({ n => $i }, { '$set' => { updated => True } }) }
        }))
    };
    my $updated-cnt = await $col.count-documents({ updated => True });
    assert $updated-cnt == $CONC, "all $CONC concurrent updates applied (got $updated-cnt)";

    # 50 concurrent mixed ops (insert + find + update per task)
    await timed "$CONC concurrent mixed ops (insert+find+update)", {
        Promise.allof((^$CONC).map(-> $i {
            start {
                await $col.insert-one({ mixed => True, i => $i });
                await $col.find-one({ n => $i % $CONC });
                await $col.update-one({ mixed => True, i => $i }, { '$set' => { done => True } });
            }
        }))
    };
    pass "$CONC concurrent mixed ops completed without error";

    # ── 13. Large Bulk Insert Stress ───────────────────────────────────────
    section 'Large Bulk Insert Stress';

    await $col.drop;
    my @large-batch = (^5_000).map(-> $i {
        { idx => $i, name => "User $i", email => "u{$i}\@test.com",
          score => $i * 1.23, active => ($i % 2 == 0) }
    });
    my $bulk-res = await timed 'insert-many (5000 docs)', { $col.insert-many(@large-batch) };
    assert $bulk-res<insertedCount> == 5_000,
        "5000 docs inserted (got {$bulk-res<insertedCount>})";

    my $big-cnt = await $col.count-documents;
    assert $big-cnt == 5_000, "count after 5000 bulk insert == 5000 (got $big-cnt)";

    # Aggregate over large collection
    my @big-agg = @(await timed 'aggregate over 5000 docs ($group by active)', {
        $col.aggregate([
            { '$group' => { _id => '$active', count => { '$sum' => 1 } } },
            { '$sort'  => { _id => 1 } },
        ])
    });
    assert @big-agg.elems == 2, "aggregate 5000-doc collection returns 2 groups";
    assert @big-agg.map(*<count>).sum == 5_000, "aggregate counts sum to 5000";

    # ── 14. Connection Pool Stress ─────────────────────────────────────────
    section 'Connection Pool Stress';

    my $pool-client = MongoDB::Fast.new(:use-pool, :max-connections(5), :min-connections(2));
    await $pool-client.connect;
    my $pool-col    = $pool-client.db($DB).collection('pool_battle');
    await $pool-col.drop;

    await timed '30 concurrent pool inserts', {
        Promise.allof((^30).map(-> $i {
            start { await $pool-col.insert-one({ pool => True, n => $i }) }
        }))
    };
    my $pool-cnt = await $pool-col.count-documents;
    assert $pool-cnt == 30, "pool: all 30 inserts landed (got $pool-cnt)";

    await timed '30 concurrent pool finds', {
        Promise.allof((^30).map(-> $i {
            start { await $pool-col.find-one({ n => $i }) }
        }))
    };
    pass 'pool: 30 concurrent finds completed without error';

    await timed '30 concurrent pool mixed ops', {
        Promise.allof((^30).map(-> $i {
            start {
                await $pool-col.update-one({ n => $i }, { '$set' => { seen => True } });
                await $pool-col.find-one({ n => $i });
            }
        }))
    };
    my $seen-cnt = await $pool-col.count-documents({ seen => True });
    assert $seen-cnt == 30, "pool: all 30 updates applied (got $seen-cnt)";

    $pool-client.close;

    # ── 15. Error / Edge Case Handling ─────────────────────────────────────
    section 'Error / Edge Case Handling';

    await $col.drop;

    # find-one on empty collection returns Nil/undefined
    my $missing = await $col.find-one({ x => 'does_not_exist' });
    assert !$missing.defined, 'find-one returns undefined for missing doc';

    # count on empty collection returns 0
    my $zero = await $col.count-documents;
    assert $zero == 0, 'count-documents on empty collection returns 0';

    # drop non-existent collection (idempotent)
    my $drop-ok = True;
    try { await $col.drop; CATCH { default { $drop-ok = False } } }
    assert $drop-ok, 'drop non-existent collection does not throw';

    # update-one on non-existent doc without upsert → matchedCount 0
    my $noop-upd = await $col.update-one({ ghost => True }, { '$set' => { x => 1 } });
    assert $noop-upd<matchedCount> == 0, 'update-one no-match → matchedCount 0';
    assert $noop-upd<modifiedCount> == 0, 'update-one no-match → modifiedCount 0';

    # delete-one on non-existent doc → deletedCount 0
    my $noop-del = await $col.delete-one({ ghost => True });
    assert $noop-del<deletedCount> == 0, 'delete-one no-match → deletedCount 0';

    # Duplicate explicit _id should throw
    use MongoDB::Fast::BSON;
    my $dup-id = MongoDB::Fast::BSON.new.generate-object-id;
    await $col.insert-one({ _id => $dup-id, dup => 1 });
    my $dup-failed = False;
    try {
        await $col.insert-one({ _id => $dup-id, dup => 2 });
        CATCH { default { $dup-failed = True } }
    }
    assert $dup-failed, 'duplicate _id insert throws an error';

    # Empty filter matches all (find)
    await $col.insert-many([{ a => 1 }, { a => 2 }, { a => 3 }]);
    my $all-cur = $col.find({});
    my @all = await $all-cur.all;
    assert @all.elems >= 3, "empty filter \{\} matches all docs (got {@all.elems})";

    # ── Cleanup ────────────────────────────────────────────────────────────
    await $col.drop;
    await $pool-col.drop;
    $client.close;

    # ── Summary ────────────────────────────────────────────────────────────
    say '';
    say '=' x 72;
    say "Results: $PASS passed, $FAIL failed  ({$PASS + $FAIL} total)";
    say '=' x 72;
    if @FAILURES {
        say '';
        say 'Failures:';
        for @FAILURES -> $f { say "  - $f" }
        say '';
    }
    exit $FAIL > 0 ?? 1 !! 0;
};

await $main;
