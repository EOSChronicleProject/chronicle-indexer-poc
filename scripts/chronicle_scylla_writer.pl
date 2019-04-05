# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libdatetime-format-iso8601-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm DBD::Cassandra

use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use DateTime;
use DateTime::Format::ISO8601;

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $port = 8800;

my $dsn = 'DBI:Cassandra:host=10.0.3.35;keyspace=eosidx';
my $db_user = 'cassandra';
my $db_password = 'cassandra';
my $ack_every = 10;

my $ok = GetOptions
    ('port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
    );


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "Options:\n",
    "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
    "  --ack=N            \[$ack_every\] Send acknowledgements every N blocks\n",
    "  --dsn=DSN          \[$dsn\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1 });
die($DBI::errstr) unless $dbh;


my $sth_get_head = $dbh->prepare('SELECT ptr FROM EOSIDX_PTR WHERE id=0');

my $sth_set_head = $dbh->prepare('UPDATE EOSIDX_PTR SET ptr=? WHERE id=0');

my $sth_wipe_blk = $dbh->prepare
    ('DELETE FROM EOSIDX_ACTIONS WHERE block_num=?');

my $sth_ins_action = $dbh->prepare
    ('INSERT INTO EOSIDX_ACTIONS ' .
     '(block_num, block_time, trx_id, global_action_seq, parent,' .
     'contract, action_name, receiver, recv_sequence) ' .
     'VALUES(?,?,?,?,?,?,?,?,?)');

my $sth_set_irrev = $dbh->prepare
    ('UPDATE EOSIDX_PTR SET ptr=? WHERE id=1');


my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $last_irreversible = 0;
my $json = JSON->new;

Net::WebSocket::Server->new(
    listen => $port,
    on_connect => sub {
        my ($serv, $conn) = @_;
        $conn->on(
            'binary' => sub {
                my ($conn, $msg) = @_;
                my ($msgtype, $opts, $js) = unpack('VVa*', $msg);
                my $data = eval {$json->decode($js)};
                if( $@ )
                {
                    print STDERR $@, "\n\n";
                    print STDERR $js, "\n";
                    exit;
                } 
                
                my $ack = process_data($msgtype, $data);
                if( $ack > 0 )
                {
                    $conn->send_binary(sprintf("%d", $ack));
                    print STDERR "ack $ack\n";
                }
            },
            'disconnect' => sub {
                my ($conn, $code) = @_;
                print STDERR "Disconnected: $code\n";
                $confirmed_block = 0;
                $unconfirmed_block = 0;
                $last_irreversible = 0;
            },
            
            );
    },
    )->start;


sub process_data
{
    my $msgtype = shift;
    my $data = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";

        $sth_get_head->execute();
        my $r = $sth_get_head->fetchall_arrayref();
        if( scalar(@{$r}) > 0 )
        {
            my $head = $r->[0][0];
            while( $head >= $block_num )
            {
                $sth_wipe_blk->execute($head);
                $head--;
            }
        }
        $sth_set_head->execute($block_num - 1);
        $confirmed_block = $block_num;
        $unconfirmed_block = 0;
        return $block_num;
    }
    elsif( $msgtype == 1003 ) # CHRONICLE_MSGTYPE_TX_TRACE
    {
        my $trace = $data->{'trace'};
        if( $trace->{'status'} eq 'executed' )
        {
            my $bt = DateTime::Format::ISO8601->parse_datetime($data->{'block_timestamp'});
            $bt->set_time_zone('UTC');
            
            my $tx = {'block_num' => $data->{'block_num'},
                      'block_time' => $bt->hires_epoch() * 1000,
                      'trx_id' => $trace->{'transaction_id'}};
            
            foreach my $atrace (@{$trace->{'traces'}})
            {
                process_atrace($tx, $atrace, 0);
            }
        }
    }
    elsif( $msgtype == 1009 ) # CHRONICLE_MSGTYPE_RCVR_PAUSE
    {
        if( $unconfirmed_block > $confirmed_block )
        {
            $confirmed_block = $unconfirmed_block;
            return $confirmed_block;
        }
    }
    elsif( $msgtype == 1010 ) # CHRONICLE_MSGTYPE_BLOCK_COMPLETED
    {
        my $block_num = $data->{'block_num'};
        $sth_set_head->execute($block_num);
        $unconfirmed_block = $block_num;
        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            if( $last_irreversible < $unconfirmed_block )
            {
                $sth_set_irrev->execute($data->{'last_irreversible'});
            }
            $confirmed_block = $unconfirmed_block;
            $last_irreversible = $data->{'last_irreversible'};
            return $confirmed_block;
        }
    }

    return 0;
}


sub process_atrace
{
    my $tx = shift;
    my $atrace = shift;
    my $parent = shift;

    my $receipt = $atrace->{'receipt'};
    my $seq = $receipt->{'global_sequence'};
    
    $sth_ins_action->execute($tx->{'block_num'}, $tx->{'block_time'}, $tx->{'trx_id'},
                             $seq, $parent, $atrace->{'account'},
                             $atrace->{'name'}, $receipt->{'receiver'}, $receipt->{'recv_sequence'});
    
    if( defined($atrace->{'inline_traces'}) )
    {
        foreach my $trace (@{$atrace->{'inline_traces'}})
        {
            process_atrace($tx, $trace, $seq);
        }
    }
}
    
    
        


   
