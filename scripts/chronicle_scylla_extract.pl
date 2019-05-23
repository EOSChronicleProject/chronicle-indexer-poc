# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libdatetime-format-iso8601-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm Cassandra::Client;



use strict;
use warnings;
use utf8;
use JSON;
use Getopt::Long;
use Cassandra::Client;

$| = 1;

my $block_num;

my $db_host = '10.0.3.35';
my $db_keyspace = 'eosidx';
my $db_user = 'cassandra';
my $db_password = 'cassandra';

my $ok = GetOptions
    (
     'block=i'   => \$block_num,
     'dbhost=s'  => \$db_host,
     'dbks=s'    => \$db_keyspace,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
    );


if( not $ok or scalar(@ARGV) > 0 or not $block_num )
{
    print STDERR "Usage: $0 --block=N [options...]\n",
    "Options:\n",
    "  --block=N          block number\n",
    "  --dbost=HOST       \[$db_host\]\n",
    "  --dbks=KEYSPACE    \[$db_keyspace\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}

my $db= Cassandra::Client->new(
    contact_points => [$db_host],
    username => $db_user,
    password => $db_password,
    keyspace => $db_keyspace,
    request_timeout => 300,
    );
$db->connect();


my $json = JSON->new->canonical->pretty;

my $result = [];

my ($r) = $db->execute('SELECT jsdata FROM traces WHERE block_num=?', [$block_num]);
my $rows = $r->rows();
foreach my $row (@{$rows})
{
    my $data = $json->decode($row->[0]);
    push(@{$result}, $data);
}


print $json->encode($result);


   
