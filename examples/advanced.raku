#!/usr/bin/env raku
# examples/advanced.raku — MongoDB::Fast advanced patterns
#
# Scenario: e-commerce order analytics
#   - bulk ingestion with timing
#   - concurrent parallel writes
#   - connection pooling
#   - complex aggregation pipelines
#   - cursor-based pagination
#   - index-backed queries
#   - upsert / replace patterns
#   - error handling

use MongoDB::Fast;

# ── helpers ───────────────────────────────────────────────────────────────────

my @CATEGORIES = <electronics clothing books food sports>;
my @STATUSES   = <pending shipped delivered cancelled>;
my @CUSTOMERS  = (1..20).map: { "cust_{$_}" };

sub rand-order(Int $i) {
    %(
        order_id  => $i,
        customer  => @CUSTOMERS.pick,
        category  => @CATEGORIES.pick,
        amount    => (100 + (^900).pick) / 10e0,  # $10–$99.9
        status    => @STATUSES.pick,
        created   => now.to-posix[0].Int - (^30 * 86400).pick,  # last 30 days
    )
}

sub section(Str $title) {
    say "";
    say "── $title " ~ "─" x (60 - $title.chars);
}

# ── main ──────────────────────────────────────────────────────────────────────

sub MAIN(Int :$orders = 5_000, Int :$batch = 500, Bool :$pool = False) {

    say "MongoDB::Fast — advanced example";
    say "orders=$orders  batch=$batch  pool={$pool ?? 'yes' !! 'no'}";

    my $m = $pool
        ?? MongoDB::Fast.new(:use-pool, max-connections => 10)
        !! MongoDB::Fast.new;
    await $m.connect;

    my $db  = $m.db('example_advanced');
    my $col = $db.collection('orders');
    my $inv = $db.collection('inventory');   # second collection for upsert demo

    await $col.drop;
    await $inv.drop;

    # ── 1. Bulk insert with timing ────────────────────────────────────────────
    section "1. Bulk insert ($orders orders, batch $batch)";

    my $t0 = now;
    my @buf;
    my $batches = 0;

    for ^$orders -> $i {
        @buf.push: rand-order($i);
        if @buf.elems == $batch {
            await $col.insert-many(@buf);
            @buf = ();
            $batches++;
        }
    }
    await $col.insert-many(@buf) if @buf;

    my $elapsed = now - $t0;
    printf "  %d docs in %.3fs → %d docs/sec\n",
        $orders, $elapsed, ($orders / $elapsed).Int;

    # ── 2. Parallel concurrent writes ────────────────────────────────────────
    section "2. Parallel concurrent inserts (20 × insert-one)";

    my $t1 = now;
    # Note: use %() not {} when $_ is in scope to force Hash, not Block
    my @p = (1..20).map: { $inv.insert-one(%(sku => "SKU-$_", stock => (^100).pick)) };
    await Promise.allof(@p);
    printf "  20 concurrent inserts in %.3fs\n", now - $t1;

    # ── 3. Indexes ────────────────────────────────────────────────────────────
    section "3. Index creation";

    await Promise.allof(
        $col.create-index({ customer => 1 }),
        $col.create-index({ category => 1, status => 1 }),
        $col.create-index({ created  => -1 }),
        $inv.create-index({ sku => 1 }, options => { unique => True, name => 'sku_unique' }),
    );
    say "  4 indexes created";

    # ── 4. Count & find-one ───────────────────────────────────────────────────
    section "4. Count & find-one";

    my $total = await $col.count-documents;
    my $pending = await $col.count-documents({ status => 'pending' });
    say "  total=$total  pending=$pending";

    my $sample = await $col.find-one(
        { status => 'delivered' },
        options => { sort => { amount => -1 } }
    );
    if $sample {
        say "  highest delivered order: #$sample<order_id> "
            ~ "\$$sample<amount> by $sample<customer>";
    }

    # ── 5. Cursor-based pagination ────────────────────────────────────────────
    section "5. Cursor pagination (top 3 pages × 5 electronics orders)";

    for ^3 -> $page {
        my $cursor = $col.find(
            { category => 'electronics' },
            options => {
                sort      => { amount => -1 },
                limit     => 5,
                skip      => $page * 5,
                projection => { order_id => 1, amount => 1, customer => 1, _id => 0 },
            }
        );
        my @rows = await $cursor.all;
        last unless @rows;
        say "  page {$page+1}: " ~ @rows.map({ "#$_<order_id> \$$_<amount>" }).join(', ');
    }

    # ── 6. Update operations ──────────────────────────────────────────────────
    section "6. Update operations";

    # Bulk-ship all pending orders
    my $r = await $col.update-many(
        { status => 'pending' },
        { '$set' => { status => 'shipped', shipped_at => now.to-posix[0].Int } }
    );
    say "  shipped $r<modifiedCount> pending orders";

    # Apply 10% discount to high-value delivered orders
    $r = await $col.update-many(
        { status => 'delivered', amount => { '$gte' => 80 } },
        { '$mul' => { amount => 0.9e0 } }
    );
    say "  discounted $r<modifiedCount> high-value delivered orders";

    # ── 7. Upsert — inventory stock sync ─────────────────────────────────────
    section "7. Upsert (inventory sync)";

    for <SKU-1 SKU-5 SKU-21 SKU-22> -> $sku {
        my $result = await $inv.update-one(
            { sku => $sku },
            { '$set' => { stock => (50 + (^50).pick), updated => now.to-posix[0].Int } },
            :upsert
        );
        my $action = $result<upsertedId>.defined ?? "inserted" !! "updated";
        say "  $sku → $action";
    }

    # ── 8. Replace ────────────────────────────────────────────────────────────
    section "8. Replace-one";

    my $old = await $inv.find-one({ sku => 'SKU-1' });
    await $inv.replace-one(
        { sku => 'SKU-1' },
        { sku => 'SKU-1', stock => 999, warehouse => 'A', updated => now.to-posix[0].Int }
    );
    my $new = await $inv.find-one({ sku => 'SKU-1' });
    say "  SKU-1 stock: $old<stock> → $new<stock>  warehouse: {$new<warehouse> // 'n/a'}";

    # ── 9. Aggregation pipelines ──────────────────────────────────────────────
    section "9. Aggregation — revenue by category";

    my $by-cat = await $col.aggregate([
        { '$match'  => { status => { '$in' => ['delivered', 'shipped'] } } },
        { '$group'  => {
            _id     => '$category',
            revenue => { '$sum' => '$amount' },
            orders  => { '$sum' => 1 },
            avg_val => { '$avg' => '$amount' },
        }},
        { '$sort'   => { revenue => -1 } },
    ]);
    for @$by-cat -> $r {
        printf "  %-12s  revenue=\$%7.2f  orders=%d  avg=\$%.2f\n",
            $r<_id>, $r<revenue>, $r<orders>, $r<avg_val>;
    }

    section "9b. Aggregation — top 5 customers by spend";

    my $top-custs = await $col.aggregate([
        { '$match'  => { status => { '$in' => ['delivered', 'shipped'] } } },
        { '$group'  => { _id => '$customer', total => { '$sum' => '$amount' }, count => { '$sum' => 1 } } },
        { '$sort'   => { total => -1 } },
        { '$limit'  => 5 },
        { '$project' => { _id => 0, customer => '$_id', total => 1, count => 1 } },
    ]);
    for @$top-custs -> $r {
        printf "  %-10s  \$%.2f  (%d orders)\n", $r<customer>, $r<total>, $r<count>;
    }

    section "9c. Aggregation — daily order volume (last 7 days)";

    my $cutoff = now.to-posix[0].Int - 7 * 86400;
    my $daily = await $col.aggregate([
        { '$match' => { created => { '$gte' => $cutoff } } },
        { '$group' => {
            _id    => { '$subtract' => [ '$created', { '$mod' => [ '$created', 86400 ] } ] },
            count  => { '$sum' => 1 },
            revenue => { '$sum' => '$amount' },
        }},
        { '$sort'  => { _id => 1 } },
    ]);
    for @$daily -> $r {
        my $date = DateTime.new($r<_id>).yyyy-mm-dd;
        printf "  %s  orders=%3d  revenue=\$%.2f\n", $date, $r<count>, $r<revenue>;
    }

    # ── 10. Error handling ────────────────────────────────────────────────────
    section "10. Error handling";

    # Duplicate key on unique index
    try {
        await $inv.insert-one({ sku => 'SKU-1', stock => 1 });
        CATCH { default { say "  caught duplicate key: {$_.message.lines[0]}" } }
    }

    # Delete and verify
    section "Cleanup";
    await $col.drop;
    await $inv.drop;
    say "  collections dropped";

    $m.close;
    say "\nDone.";
}
