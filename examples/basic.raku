#!/usr/bin/env raku
# examples/basic.raku — MongoDB::Fast feature walkthrough

use MongoDB::Fast;

sub MAIN() {
    my $m = MongoDB::Fast.new;
    await $m.connect;
    say "Connected: " ~ ((await $m.ping) ?? "OK" !! "FAIL");

    my $col = $m.db('example').collection('people');
    await $col.drop;  # start clean

    # ── Insert ────────────────────────────────────────────────────────────────
    say "\n-- insert-one";
    my $r = await $col.insert-one({ name => 'Alice', age => 30, score => 85 });
    say "insertedId: $r<insertedId>";

    say "\n-- insert-many";
    my @people = (
        { name => 'Bob',   age => 25, score => 72 },
        { name => 'Carol', age => 35, score => 91 },
        { name => 'Dave',  age => 28, score => 60 },
    );
    $r = await $col.insert-many(@people);
    say "insertedCount: $r<insertedCount>";

    # ── Count ─────────────────────────────────────────────────────────────────
    say "\n-- count";
    my $n = await $col.count-documents;
    say "total docs: $n";

    # ── Find ──────────────────────────────────────────────────────────────────
    say "\n-- find-one";
    my $doc = await $col.find-one({ name => 'Alice' });
    say "found: $doc<name> age=$doc<age>";

    say "\n-- find all (cursor)";
    my $cursor = $col.find({});
    while my $d = await $cursor.next {
        say "  $d<name> score=$d<score>";
    }

    say "\n-- find with filter + sort";
    my @results = await $col.find(
        { age => { '$gte' => 28 } },
        options => { sort => { score => -1 } }
    ).all;
    say "  age>=28 sorted by score desc:";
    say "  $_<name> score=$_<score>" for @results;

    # ── Update ────────────────────────────────────────────────────────────────
    say "\n-- update-one";
    $r = await $col.update-one({ name => 'Bob' }, { '$set' => { score => 99 } });
    say "modified: $r<modifiedCount>";

    say "\n-- update-many (give everyone a bonus)";
    $r = await $col.update-many({}, { '$inc' => { score => 5 } });
    say "modified: $r<modifiedCount>";

    # ── Upsert ────────────────────────────────────────────────────────────────
    say "\n-- upsert";
    $r = await $col.update-one(
        { name => 'Eve' },
        { '$set' => { age => 22, score => 50 } },
        :upsert
    );
    say "upsertedId: {$r<upsertedId> // 'n/a (matched existing)'}";

    # ── Aggregation ───────────────────────────────────────────────────────────
    say "\n-- aggregate (total and avg score)";
    my $agg = await $col.aggregate([
        { '$group' => {
            _id   => Nil,
            avg   => { '$avg' => '$score' },
            total => { '$sum' => '$score' },
            count => { '$sum' => 1 },
        }},
    ]);
    if $agg[0] -> $a {
        say "  count=$a<count> total=$a<total> avg={$a<avg>.fmt('%.1f')}";
    }

    # ── Index ─────────────────────────────────────────────────────────────────
    say "\n-- create-index";
    await $col.create-index({ name => 1 }, options => { unique => True, name => 'name_unique' });
    say "index created";

    # ── Delete ────────────────────────────────────────────────────────────────
    say "\n-- delete-one";
    $r = await $col.delete-one({ name => 'Dave' });
    say "deleted: $r<deletedCount>";

    say "\n-- delete-many (score < 70)";
    $r = await $col.delete-many({ score => { '$lt' => 70 } });
    say "deleted: $r<deletedCount>";

    say "\n-- final count: " ~ (await $col.count-documents);

    # ── Cleanup ───────────────────────────────────────────────────────────────
    await $col.drop;
    $m.close;
    say "\nDone.";
}
