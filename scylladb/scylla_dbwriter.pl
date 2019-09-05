# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libdatetime-format-iso8601-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm Cassandra::Client;



use strict;
use warnings;
use JSON;
use Getopt::Long;
use Cassandra::Client;
use Time::Local 'timegm_nocheck';
use Time::HiRes qw (time);

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $port = 8800;

my $interactive;
my $interactive_range;
    
my $db_host = '10.0.3.1';
my $db_keyspace = 'eos';
my $db_user = 'cassandra';
my $db_password = 'cassandra';
my $ack_every = 120;

my $idx_retention = 10000000;
my $trace_blocks = $idx_retention;

my $ok = GetOptions
    ('port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'traceblk=i' => \$trace_blocks,
     'interactive' => \$interactive,
     'range=s'   => \$interactive_range,
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
    "  --interactive      run interactive Chronicle consumer\n",
    "  --range=M-N        range of block numbers in interactive mode\n",
    "  --dbost=HOST       \[$db_host\]\n",
    "  --dbks=KEYSPACE    \[$db_keyspace\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}

my $end_block;

if( $interactive )
{
    if( not defined($interactive_range) )
    {
        print STDERR "range is mandatory in interactive mode\n";
        exit 1;
    }

    if( $interactive_range !~ /^(\d+)-(\d+)$/ )
    {
        print STDERR "invalid format for blocks range: $interactive_range\n";
        exit 1;
    }

    my $start_block = $1;
    $end_block = $2;
    if( $end_block < $start_block )
    {
        print STDERR "invalid range: end block cannot be lower than start\n";
        exit 1;
    }
}



my $db;

sub connect_db
{
    $db = Cassandra::Client->new(
        contact_points => [$db_host],
        username => $db_user,
        password => $db_password,
        keyspace => $db_keyspace,
        request_timeout => 300,
        );
    $db->connect();
}

connect_db();

my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $last_irreversible = 0;
my $json = JSON->new;
my @batch;
my @traces_batch;
my @actions_for_block;

my $lowest_action_block;
my $lowest_trace_block;

if( not $interactive )
{
    my ($r) = $db->execute('SELECT id,ptr FROM pointers');
    my $rows = $r->rows();
    foreach my $row (@{$rows})
    {
        if( $row->[0] == 2 )
        {
            $lowest_trace_block = $row->[1];
            printf STDERR "Lowest block with JSON traces: $lowest_trace_block\n";
        }
        elsif( $row->[0] == 3 )
        {
            $lowest_action_block = $row->[1];
            printf STDERR "Lowest block in actions: $lowest_action_block\n";
        }
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
     

my $insert_action =
    'INSERT INTO actions ' .
    '(block_num,block_time,trx_id,global_action_seq,action_ord,crtr_action_ord,' .
    'contract,action_name,receiver) ' .
    'VALUES(?,?,?,?,?,?,?,?,?)';


my $dbupdates_counter = 0;
my $blocks_counter = 0;
my $counter_start = time();


Net::WebSocket::Server->new(
    listen => $port,
    on_connect => sub {
        my ($serv, $conn) = @_;
        $conn->on(
            'ready' => sub {
                my ($conn, $handshake) = @_;
                if( $interactive )
                {
                    $conn->send_binary($interactive_range);
                }
            },
                
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

                my $done = 0;
                my $ack = 0;
                while( not $done )
                {
                    eval {
                        $ack = process_data($msgtype, $data, \$js);
                        if( $ack > 0 )
                        {
                            if( $interactive )
                            {
                                if( $ack >= $end_block - 1 )
                                {
                                    printf STDERR ("reached end block %d, disconnecting\n", $end_block);
                                    $conn->disconnect(0, 0);
                                    $serv->shutdown();
                                }
                            }
                            else
                            {
                                $conn->send_binary(sprintf("%d", $ack));
                            }
                        }
                    };
                    
                    if( $@ )
                    {
                        print STDERR $@, "\n", "retrying\n";
                        $db->shutdown();
                        sleep(8);
                        do {
                            eval { sleep(2); connect_db(); };
                        } while ($@);
                    }
                    else
                    {
                        $done = 1;
                    }
                }
                    
                my $period = time() - $counter_start;
                if( $ack > 0 and $period > 0 )
                {
                    printf STDERR ("saved %d, period: %.2f, updates/s: %.2f, blocks/s: %.2f\n",
                                   $ack, $period, $dbupdates_counter/$period, $blocks_counter/$period);
                    $counter_start = time();
                    $dbupdates_counter = 0;
                    $blocks_counter = 0;
                }
            },
            'disconnect' => sub {
                my ($conn, $code) = @_;
                print STDERR "Disconnected\n";
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

    if( $msgtype == 1001 and not $interactive ) # CHRONICLE_MSGTYPE_FORK
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
                    push_upd(['DELETE FROM traces WHERE block_num=?', [$lowest_trace_block]]);
                    $lowest_trace_block++;
                    push_upd(['UPDATE pointers SET ptr=? WHERE id=2', [$lowest_trace_block]]);
                }
                push_upd(['DELETE FROM pointers WHERE id=2', []]);
            }
                
            printf STDERR "Head: $head - deleting $block_num to $head\n";
            while( $head >= $block_num )
            {
                push_upd(['DELETE FROM actions WHERE block_num=?', [$head]]);
                $head--;
            }

            push_upd(['UPDATE pointers SET ptr=? WHERE id=0', [$block_num - 1]]);
            write_batch();
        }
        
        $confirmed_block = $block_num - 1;
        $unconfirmed_block = 0;
        return $confirmed_block;
    }
    elsif( $msgtype == 1003 ) # CHRONICLE_MSGTYPE_TX_TRACE
    {
        my $trace = $data->{'trace'};
        if( $trace->{'status'} eq 'executed' )
        {
            my $block_num = $data->{'block_num'};
            my $trx_id = pack('H*', $trace->{'id'});

            my ($year, $mon, $mday, $hour, $min, $sec, $msec) =
                split(/[-:.T]/, $data->{'block_timestamp'});

            my $epoch = timegm_nocheck($sec, $min, $hour, $mday, $mon-1, $year);
                
            my $tx = [$block_num, 1000 * $epoch + $msec, $trx_id];
            
            foreach my $atrace (@{$trace->{'action_traces'}})
            {
                process_atrace($tx, $atrace);
            }

            if( $writing_traces )
            {
                push_trace(
                    ['INSERT INTO traces (block_num, trx_id, jsdata) VALUES(?,?,?)',
                     [$block_num, $trx_id, ${$jsptr}]]);
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
        # printf STDERR ("rcv %d\n", $block_num);
        
        if( not $interactive )
        {            
            my $last_irreversible = $data->{'last_irreversible'};
            
            if( $block_num >= $last_irreversible - $idx_retention )
            {
                push(@batch, @actions_for_block);
                @actions_for_block = ();
                
                if( not $writing_traces )
                {
                    if( $block_num >= $last_irreversible - $trace_blocks )
                    {
                        $lowest_trace_block = $block_num+1;
                        $writing_traces = 1;
                        push_upd(['UPDATE pointers SET ptr=? WHERE id=2', [$lowest_trace_block]]);
                        printf STDERR "Started writing JSON traces from block $lowest_trace_block\n";
                    }
                }
                else
                {
                    while( $lowest_trace_block < $last_irreversible - $trace_blocks )
                    {
                        push_upd(['DELETE FROM traces WHERE block_num=?', [$lowest_trace_block]]);
                        $lowest_trace_block++;
                        push_upd(['UPDATE pointers SET ptr=? WHERE id=2', [$lowest_trace_block]]);
                        if( $lowest_trace_block > $block_num )
                        {
                            $lowest_trace_block = undef;
                            $writing_traces = 0;
                            push_upd(['DELETE FROM pointers WHERE id=2', []]);
                            last;
                        }
                    }
                }
                
                if( not defined($lowest_action_block) )
                {
                    $lowest_action_block = $block_num;
                    push_upd(['UPDATE pointers SET ptr=? WHERE id=3', [$lowest_action_block]]);
                }      

                while( $lowest_action_block < $last_irreversible - $idx_retention )
                {
                    push_upd(['DELETE FROM actions WHERE block_num=?', [$lowest_action_block]]);
                    $lowest_action_block++;
                    push_upd(['UPDATE pointers SET ptr=? WHERE id=3', [$lowest_action_block]]);
                }
                
                push_upd(['UPDATE pointers SET ptr=? WHERE id=0', [$block_num]]);
                push_upd(['UPDATE pointers SET ptr=? WHERE id=1', [$last_irreversible]]);
            }
        }
        else
        {
            my ($r) = $db->execute('SELECT count(*) FROM actions WHERE block_num=?', [$block_num]);
            my $rows = $r->rows();
            if( $rows->[0][0] != scalar(@actions_for_block) )
            {
                push(@batch, @actions_for_block);
                @actions_for_block = ();
            }
        }
        
        $blocks_counter++;
        write_batch();
        
        $unconfirmed_block = $block_num;
        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            $confirmed_block = $unconfirmed_block;
            return $confirmed_block;
        }

        if( $interactive and $block_num >= $end_block - 1 )
        {
            return $block_num;
        }
    }

    return 0;
}


sub push_upd
{
    push(@batch, $_[0]);
}

sub push_trace
{
    push(@traces_batch, $_[0]);
}



sub write_batch
{
    $dbupdates_counter += scalar(@traces_batch) + scalar(@batch);
    
    while( scalar(@traces_batch) > 0 )
    {
        $db->execute(@{shift(@traces_batch)});
    }

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

    my $receipt = $atrace->{'receipt'};
    my $seq = $receipt->{'global_sequence'};
    my $act = $atrace->{'act'};
    my $contract = $act->{'account'};
    my $aname = $act->{'name'};
    my $data = $act->{'data'};

    my %receivers = ($receipt->{'receiver'} => 1);

    if( $contract eq 'eosio' and ref($data) eq 'HASH' )
    {
        if( defined($eosio_act_recipients{$aname}) )
        {
            foreach my $field (@{$eosio_act_recipients{$aname}})
            {
                my $rcvr = $data->{$field};
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
        push(@actions_for_block,
             [ $insert_action, 
               [@{$tx}, $seq, $atrace->{'action_ordinal'}, $atrace->{'creator_action_ordinal'},
                $contract, $aname, $rcvr]]);
    }    
}
    
    
        


   
