# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm DBD::MariaDB

use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;
    
$| = 1;

my $port = 8800;

my $dsn = 'DBI:MariaDB:database=eosidx;host=localhost';
my $db_user = 'eosidx';
my $db_password = 'guugh3Ei';
my $commit_every = 100;

my $ok = GetOptions
    ('port=i'    => \$port,
     'ack=i'     => \$commit_every,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
    );


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "Options:\n",
    "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
    "  --ack=N            \[$commit_every\] Send acknowledgements every N blocks\n",
    "  --dsn=DSN          \[$dsn\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mariadb_server_prepare => 1});
die($DBI::errstr) unless $dbh;


my $sth_ins_trx = $dbh->prepare
    ('INSERT INTO EOSIDX_TRX (trx_id, block_time, block_num, irreversible) VALUES(?,?,?,?)');

my $sth_wipe = $dbh->prepare
    ('DELETE FROM EOSIDX_TRX WHERE block_num >= ?');

my $sth_ins_action = $dbh->prepare
    ('INSERT INTO EOSIDX_ACTIONS ' .
     '(global_action_seq, parent, trx_seq_num,' .
     'contract, action_name, receiver, recv_sequence) ' .
     'VALUES(?,?,?,?,?,?,?)');

my $sth_set_irrev = $dbh->prepare
    ('UPDATE EOSIDX_TRX SET irreversible=1 WHERE irreversible=0 AND block_num<=?');


my $committed_block = 0;
my $uncommitted_block = 0;
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
        $sth_wipe->execute($block_num);
        $dbh->commit();
        $committed_block = $block_num;
        return $block_num;
    }
    elsif( $msgtype == 1002 ) # CHRONICLE_MSGTYPE_BLOCK
    {
        $last_irreversible = $data->{'last_irreversible'};
        if( $last_irreversible < $uncommitted_block )
        {
            $sth_set_irrev->execute($last_irreversible);
        }
    }
    elsif( $msgtype == 1003 ) # CHRONICLE_MSGTYPE_TX_TRACE
    {
        my $trace = $data->{'trace'};
        if( $trace->{'status'} eq 'executed' )
        {
            my $block_num = $data->{'block_num'};
            my $irreversible = ($last_irreversible >= $block_num)?1:0;
            
            $sth_ins_trx->execute($trace->{'transaction_id'}, $data->{'block_timestamp'},
                                  $block_num, $irreversible);
            my $trx_seq = $sth_ins_trx->last_insert_id();
            
            foreach my $atrace (@{$trace->{'traces'}})
            {
                process_atrace($trx_seq, $atrace, 0);
            }
        }
    }
    elsif( $msgtype == 1009 ) # CHRONICLE_MSGTYPE_RCVR_PAUSE
    {
        if( $uncommitted_block > $committed_block )
        {
            $dbh->commit();
            $committed_block = $uncommitted_block;
            return $committed_block;
        }
    }
    elsif( $msgtype == 1010 ) # CHRONICLE_MSGTYPE_BLOCK_COMPLETED
    {
        $uncommitted_block = $data->{'block_num'};
        if( $uncommitted_block - $committed_block >= $commit_every )
        {
            $dbh->commit();
            $committed_block = $uncommitted_block;
            return $committed_block;
        }
    }

    return 0;
}


sub process_atrace
{
    my $trx_seq = shift;
    my $atrace = shift;
    my $parent = shift;

    my $receipt = $atrace->{'receipt'};
    my $seq = $receipt->{'global_sequence'};
    
    $sth_ins_action->execute($seq, $parent, $trx_seq, $atrace->{'account'},
                             $atrace->{'name'}, $receipt->{'receiver'}, $receipt->{'recv_sequence'});
    
    if( defined($atrace->{'inline_traces'}) )
    {
        foreach my $trace (@{$atrace->{'inline_traces'}})
        {
            process_atrace($trx_seq, $trace, $seq);
        }
    }
}
    
    
        


   
