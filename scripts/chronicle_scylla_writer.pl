# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libdatetime-format-iso8601-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm Cassandra::Client;

use strict;
use warnings;
use JSON;
use Getopt::Long;
use Cassandra::Client;
use DateTime;
use DateTime::Format::ISO8601;

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $port = 8800;

my $db_host = '10.0.3.35';
my $db_keyspace = 'eosidx';
my $db_user = 'cassandra';
my $db_password = 'cassandra';
my $ack_every = 10;
my $trace_blocks = 7200*24*7;

my $ok = GetOptions
    ('port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'traceblk=i' => $trace_blocks,
     'dbhost=s'  => \$db_host,
     'dbks=s'    => \$db_keyspace,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
    );


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "Options:\n",
    "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
    "  --ack=N            \[$ack_every\] Send acknowledgements every N blocks\n",
    "  --traceblk=N       \[$trace_blocks\] Keep JSON traces for so many blocks from head\n",
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


my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $last_irreversible = 0;
my $json = JSON->new;
my @batch;

my $lowest_trace_block;
{
    my ($r) = $db->execute('SELECT ptr FROM pointers WHERE id=2');
    my $rows = $r->rows();
    if( scalar(@{$rows}) > 0 )
    {
        $lowest_trace_block = $rows->[0][0];
        printf STDERR "Lowest block with JSON traces: $lowest_trace_block\n";
    }
}


my $writing_traces = defined($lowest_trace_block)?1:0;

my %eosio_act_recipients =
    (
     'newaccount'    => ['name'],
     'setcode'       => ['account'],
     'setabi'        => ['account'],
     'updateauth'    => ['account'],
     'deleteauth'    => ['account'],
     'linkauth'      => ['account'],
     'unlinkauth'    => ['account'],
     'buyrambytes'   => ['payer', 'receiver'],
     'buyram'        => ['payer', 'receiver'],
     'sellram'       => ['account'],
     'delegatebw'    => ['from', 'receiver'],
     'undelegatebw'  => ['from', 'receiver'],
     'unlinkauth'    => ['owner'],
     'regproducer'   => ['producer'],
     'unregprod'     => ['producer'],
     'regproxy'      => ['proxy'],
     'voteproducer'  => ['voter', 'proxy'],
     'claimrewards'  => ['owner'],
    );
     
     



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
                
                my $ack = process_data($msgtype, $data, \$js);
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
    my $jsptr = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";

        my ($r) = $db->execute('SELECT ptr FROM pointers WHERE id=0');
        my $rows = $r->rows();
        if( scalar(@{$rows}) > 0 )
        {
            my $head = $rows->[0][0];
            if( $writing_traces and $lowest_trace_block >= $block_num )
            {
                printf STDERR "Fork block $block_num is below the lowest trace block $lowest_trace_block\n";
                while( $lowest_trace_block <= $head )
                {
                    push(@batch, ['DELETE FROM traces WHERE block_num=?', [$lowest_trace_block]]);
                    $lowest_trace_block++;
                    push(@batch, ['UPDATE pointers SET ptr=? WHERE id=2', [$lowest_trace_block]]);
                }
                push(@batch, ['DELETE FROM pointers WHERE id=2', []]);
            }
                
            printf STDERR "Head: $head - deleting $block_num to $head\n";
            while( $head >= $block_num )
            {
                push(@batch, ['DELETE FROM actions WHERE block_num=?', [$head]]);
                $head--;
            }
        }

        push(@batch, ['UPDATE pointers SET ptr=? WHERE id=0', [$block_num - 1]]);
        write_batch();
        
        $confirmed_block = $block_num;
        $unconfirmed_block = 0;
        return $block_num;
    }
    elsif( $msgtype == 1003 ) # CHRONICLE_MSGTYPE_TX_TRACE
    {
        my $trace = $data->{'trace'};
        if( $trace->{'status'} eq 'executed' )
        {
            my $block_num = $data->{'block_num'};
            my $trx_id = $trace->{'transaction_id'};
            my $bt = DateTime::Format::ISO8601->parse_datetime($data->{'block_timestamp'});
            $bt->set_time_zone('UTC');
                
            my $tx = {'block_num' => $block_num,
                      'block_time' => 1000 * $bt->hires_epoch(),
                      'trx_id' => $trx_id};
            
            foreach my $atrace (@{$trace->{'traces'}})
            {
                process_atrace($tx, $atrace, 0);
            }

            
            if( $writing_traces )
            {
                $db->execute('INSERT INTO traces (block_num, trx_id, jsdata) VALUES(?,?,?)',
                             [$block_num, $trx_id, ${$jsptr}]);
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
        my $last_irreversible = $data->{'last_irreversible'};

        if( not $writing_traces )
        {
            if( $block_num + $trace_blocks >= $last_irreversible )
            {
                $lowest_trace_block = $block_num+1;
                $writing_traces = 1;
                push(@batch, ['UPDATE pointers SET ptr=? WHERE id=2', [$lowest_trace_block]]);
                printf STDERR "Started writing JSON traces from block $lowest_trace_block\n";
            }
        }
        else
        {
            while( $lowest_trace_block + $trace_blocks < $last_irreversible )
            {
                push(@batch, ['DELETE FROM traces WHERE block_num=?', [$lowest_trace_block]]);
                $lowest_trace_block++;
                push(@batch, ['UPDATE pointers SET ptr=? WHERE id=2', [$lowest_trace_block]]);
                if( $lowest_trace_block > $block_num )
                {
                    $lowest_trace_block = undef;
                    $writing_traces = 0;
                    push(@batch, ['DELETE FROM pointers WHERE id=2', []]);
                    last;
                }
            }
        }
                
        push(@batch, ['UPDATE pointers SET ptr=? WHERE id=0', [$block_num]]);
        push(@batch, ['UPDATE pointers SET ptr=? WHERE id=1', [$last_irreversible]]);
        write_batch();
        
        $unconfirmed_block = $block_num;
        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            $confirmed_block = $unconfirmed_block;
            return $confirmed_block;
        }
    }

    return 0;
}


sub write_batch
{
    while( scalar(@batch) > 0 )
    {
        my @job;
        while( scalar(@job) < 10 and scalar(@batch) > 0 )
        {
            push(@job, shift(@batch));
        }
        $db->batch(\@job);
    }
}
    
     


sub process_atrace
{
    my $tx = shift;
    my $atrace = shift;
    my $parent = shift;

    my $receipt = $atrace->{'receipt'};
    my $seq = $receipt->{'global_sequence'};

    my %receivers = ($receipt->{'receiver'} => 1);

    if( $atrace->{'account'} eq 'eosio' and ref($atrace->{'data'}) eq 'HASH' )
    {
        my $aname = $atrace->{'name'};
        if( defined($eosio_act_recipients{$aname}) )
        {
            foreach my $field (@{$eosio_act_recipients{$aname}})
            {
                my $rcvr = $atrace->{'data'}{$field};
                if( defined($rcvr) and $rcvr ne '' )
                {
                    $receivers{$rcvr} = 1;
                }
            }
        }
    }

    foreach my $auth (@{$atrace->{'authorization'}})
    {
        $receivers{$auth->{'actor'}} = 1;
    }
    
    foreach my $rcvr (keys %receivers)
    {   
        push(@batch,
             ['INSERT INTO actions ' .
              '(block_num, block_time, trx_id, global_action_seq, parent,' .
              'contract, action_name, receiver) ' .
              'VALUES(?,?,?,?,?,?,?,?)',
              [$tx->{'block_num'}, $tx->{'block_time'}, $tx->{'trx_id'},
               $seq, $parent, $atrace->{'account'},
               $atrace->{'name'}, $rcvr]]);
    }
        
    if( defined($atrace->{'inline_traces'}) )
    {
        foreach my $trace (@{$atrace->{'inline_traces'}})
        {
            process_atrace($tx, $trace, $seq);
        }
    }
}
    
    
        


   
