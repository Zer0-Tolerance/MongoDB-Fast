use MongoDB::Fast;

# plan 1;

sub MAIN (Int $k=1000,Int $max=1500) {
  my $start = now;
  my $m = MongoDB::Fast.new;
  await $m.connect;
  my $col = $m.db('test').collection('test');
  my @p;
  my @doc;
  for ^$k -> $i {
    # @p.push: $col.insert-one({foo => 'a'});
    # await @p if $i %% $max;
    # Bulk
    @doc.push: { ip => $i };
    if $i > 0 and $i %% $max {
      # dd "inserting $i";
      # dd @doc.elems;
      @p.push: $col.insert-many(@doc);
      await @p;
      @doc=();
      @p=();
      # await @p;
    }
  }
  await $col.insert-many(@doc) if @doc.elems > 0;
  # await $col.insert-one({ foo => "a" }) for ^$k;
  say "exec took: " ~ (now - $start) ~ " sec speed: " ~ ($k / (now - $start)).Int ~ " inserts/sec";
  await $col.delete-many({ ip => {'$exists' => True} });
}
