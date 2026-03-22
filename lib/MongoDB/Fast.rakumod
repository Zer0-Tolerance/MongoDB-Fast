use v6.d;
use MongoDB::Fast::Connection;
use MongoDB::Fast::Database;
use MongoDB::Fast::BSON;

unit class MongoDB::Fast;

has Str $.host = '127.0.0.1';
has Int $.port = 27017;
has Str $.username;
has Str $.password;
has Bool $.use-pool = False;
has Int $.max-connections = 10;
has Bool $.enable-auto-reconnect = True;
has Int $.max-reconnect-attempts = 5;
has Num $.initial-retry-delay = 0.1e0;
has Int $.connection-timeout = 10;

has MongoDB::Fast::Connection $!connection;
has $!pool;

submethod BUILD(
    Str :$!host = '127.0.0.1',
    Int :$!port = 27017,
    Str :$!username,
    Str :$!password,
    Bool :$!use-pool = False,
    Int :$!max-connections = 10,
    Bool :$!enable-auto-reconnect = True,
    Int :$!max-reconnect-attempts = 5,
    Num :$!initial-retry-delay = 0.1e0,
    Int :$!connection-timeout = 10,
) {
    $!connection = MongoDB::Fast::Connection.new(
        host                    => $!host,
        port                    => $!port,
        enable-auto-reconnect   => $!enable-auto-reconnect,
        max-reconnect-attempts  => $!max-reconnect-attempts,
        initial-retry-delay     => $!initial-retry-delay,
        connection-timeout      => $!connection-timeout,
        |($!username ?? (username => $!username, password => $!password) !! ()),
    ) unless $!use-pool;
}

method connect(--> Promise) {
    if $!use-pool {
        $!pool = MongoDB::Fast::Connection::Pool.new(
            max-connections  => $!max-connections,
            connection-params => %(
                host => $!host,
                port => $!port,
                enable-auto-reconnect   => $!enable-auto-reconnect,
                max-reconnect-attempts  => $!max-reconnect-attempts,
                initial-retry-delay     => $!initial-retry-delay,
                connection-timeout      => $!connection-timeout,
                |($!username ?? (username => $!username, password => $!password) !! ()),
            ),
        );
        return Promise.kept(True);
    } else {
        return $!connection.connect;
    }
}

method db(Str $name --> MongoDB::Fast::Database) {
    if $!use-pool {
        return MongoDB::Fast::Database.new(
            name => $name,
            pool => $!pool,
        );
    } else {
        return MongoDB::Fast::Database.new(
            name       => $name,
            connection => $!connection,
        );
    }
}

method connection(--> MongoDB::Fast::Connection) { $!connection }

method ping(--> Promise) {
    $!connection.ping;
}

method close() {
    $!connection.close if $!connection;
    $!pool.close-all   if $!pool;
}
