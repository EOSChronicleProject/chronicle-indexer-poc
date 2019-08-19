# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libdatetime-format-iso8601-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm Cassandra::Client;



use strict;
use warnings;
use JSON;
use Getopt::Long;
use Time::Local 'timegm_nocheck';
use Time::HiRes qw (time);
use LWP::UserAgent;
use LWP::Authen::Basic;

use Couchbase::Bucket;
use Couchbase::Document;

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $port = 8800;
my $ack_every = 100;

my $interactive;
my $interactive_range;

my $dbhost = '127.0.0.1';
my $bucket = 'histidx';
my $dbuser = 'Administrator';
my $dbpw = 'password';

my $trace_blocks = 7200*24*7;

my $ok = GetOptions
    ('port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'traceblk=i' => \$trace_blocks,
     'interactive' => \$interactive,
     'range=s'   => \$interactive_range,
     'dbhost=s'  => \$dbhost,
     'bucket=s'  => \$bucket,
     'dbuser=s'  => \$dbuser,
     'dbpw=s'    => \$dbpw,
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
    "  --dbhost=HOST      \[$dbhost] Couchbase host\n",
    "  --bucket=NAME      \[$bucket] Couchbase bucket\n",
    "  --dbuser=USER      \[$dbuser\] Couchbase user\n",
    "  --dbpw=PW          \[$dbpw\] Couchbase password\n";
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


my $cb = Couchbase::Bucket->new('couchbase://' . $dbhost . '/' . $bucket,
                                {'username' => $dbuser, 'password' => $dbpw});

sub get_pointer
{
    my $pt = shift;
    my $doc = Couchbase::Document->new('pointer:' . $pt);
    $cb->get($doc);
    if( $doc->is_ok() )
    {
        return $doc->value()->{'ptval'};
    }
    return;
}

sub set_pointer_doc
{
    my $pt = shift;
    my $val = shift;
    return Couchbase::Document->new('pointer:' . $pt, {'ptval' => $val});
}

sub set_pointer
{
    my $pt = shift;
    my $val = shift;
    my $doc = set_pointer_doc($pt, $val);
    $cb->upsert($doc);
    if (!$doc->is_ok)
    {
        die("Could not store document: " . $doc->errstr);
    }
}



my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $irreversible = 0;
my $last_checked_indexer = 0;

my $json = JSON->new;
my @batch;

my $lowest_trace_block;
if( not $interactive )
{
    $lowest_trace_block = get_pointer('lowest_trace_block');
    if( defined($lowest_trace_block) )
    {
        printf STDERR "Lowest block with JSON traces: $lowest_trace_block\n";
    }
}


my $writing_traces = (defined($lowest_trace_block) and $lowest_trace_block > 0)?1:0;

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
                $irreversible = 0;
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

        $cb->query_slurp('DELETE FROM ' . $bucket . ' WHERE type=\'action_upd\' ' .
                         'AND TONUM(block_num)>=' . $block_num,
                         {}, {'scan_consistency' => '"request_plus"'});

        $cb->query_slurp('DELETE FROM ' . $bucket . ' WHERE type=\'trace_upd\' ' .
                         'AND TONUM(block_num)>=' . $block_num,
                         {}, {'scan_consistency' => '"request_plus"'});
        
        
        $confirmed_block = $block_num - 1;
        $unconfirmed_block = $block_num - 1;
        return $confirmed_block;
    }
    elsif( $msgtype == 1003 ) # CHRONICLE_MSGTYPE_TX_TRACE
    {
        my $trace = $data->{'trace'};
        if( $trace->{'status'} eq 'executed' )
        {
            my $block_num = $data->{'block_num'};
            my $trx_id = $trace->{'id'};

            my $upd = 0;
            if( not $interactive and $irreversible > 0 and $block_num > $irreversible )
            {
                $upd = 1;
            }
            
            my $tx = {'block_num' => $block_num,
                      'block_time' => $data->{'block_timestamp'},
                      'trx_id' => $trx_id,
                      'type' => ($upd ? 'action_upd':'action') };
            
            foreach my $atrace (@{$trace->{'action_traces'}})
            {
                process_atrace($tx, $atrace);
            }

            if( $writing_traces )
            {
                $data->{'type'} = ($upd ? 'trace_upd':'trace');
                push(@batch, Couchbase::Document->new('trace:' . $trx_id, $data));
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
        # printf STDERR ("rcv %d\n", $block_num);
        
        if( not $interactive )
        {
            if( not $writing_traces )
            {
                if( $block_num + $trace_blocks >= $last_irreversible )
                {
                    $lowest_trace_block = $block_num+1;
                    $writing_traces = 1;
                    push(@batch, set_pointer_doc('lowest_trace_block', $lowest_trace_block));
                    printf STDERR "Started writing JSON traces from block $lowest_trace_block\n";
                }
            }
            else
            {
                $cb->query_slurp('DELETE FROM ' . $bucket . ' WHERE type=\'trace\' ' .
                                 'AND TONUM(block_num) < ' . ($last_irreversible - $trace_blocks));

                if( $block_num + $trace_blocks < $last_irreversible )
                {
                    $lowest_trace_block = 0;
                    $writing_traces = 0;
                    push(@batch, set_pointer_doc('lowest_trace_block', 0));
                }                    
            }
        }
        
        $blocks_counter++;
        write_batch();

        if( not $interactive )
        {            
            if( $block_num >= $last_irreversible and $last_irreversible > $irreversible )
            {
                $cb->query_slurp('UPDATE ' . $bucket . ' SET type=\'action\' WHERE type=\'action_upd\' ' .
                                 'AND TONUM(block_num)<=' . $last_irreversible);
                
                $cb->query_slurp('UPDATE ' . $bucket . ' SET type=\'trace\' WHERE type=\'trace_upd\' ' .
                                 'AND TONUM(block_num)<=' . $last_irreversible);
            }
            $irreversible = $last_irreversible;
        }

        check_indexer($block_num);
        
        $unconfirmed_block = $block_num;
        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            if( not $interactive )
            {
                push(@batch, set_pointer_doc('block_num', $block_num));
                push(@batch, set_pointer_doc('last_irreversible', $last_irreversible));
            }
            
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



sub write_batch
{
    $dbupdates_counter += scalar(@batch);

    # print STDERR "BATCH started\n";
    my $ctx = $cb->batch();
    foreach my $doc (@batch)
    {
        # print STDERR $doc->id(), "\n";
        $ctx->upsert($doc);
    }
    $ctx->wait_all();
    @batch = ();
}
    
     


sub process_atrace
{
    my $tx = shift;
    my $atrace = shift;

    my $act = $atrace->{'act'};
    my $contract = $act->{'account'};
    my $receipt = $atrace->{'receipt'};
    my $seq = $receipt->{'global_sequence'};
    my $aname = $act->{'name'};
    my $data = $act->{'data'};
    
    my %action_docdata = (
        'global_seq' => $seq,
        'contract' => $contract,
        'action' => $aname,
        'action_ord' => $atrace->{'action_ordinal'},
        'crtr_action_ord' => $atrace->{'creator_action_ordinal'},
        );
    
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

        if( $aname eq 'newaccount' )
        {
            my $name = $data->{'name'};
            if( not defined($name) )
            {
                # workaround for https://github.com/EOSIO/eosio.contracts/pull/129
                $name = $data->{'newact'};
            }


            foreach my $perm ('owner', 'active')
            {
                push(@batch, Couchbase::Document->new(join(':', 'auth', $name, $perm),
                                                      {
                                                          'type' => 'auth',
                                                          'account' => $name,
                                                          'permission' => $perm,
                                                          'auth' => $data->{$perm},
                                                      }));
            }
            write_batch();
        }
        elsif( $aname eq 'updateauth' )
        {
            my $name = $data->{'account'};
            my $perm = $data->{'permission'};
            push(@batch, Couchbase::Document->new(join(':', 'auth', $name, $perm),
                                                  {
                                                      'type' => 'auth',
                                                      'account' => $name,
                                                      'permission' => $perm,
                                                      'auth' => $data->{'auth'},
                                                  }));
            write_batch();
        }
        elsif( $aname eq 'deleteauth' )
        {
            write_batch();
            my $name = $data->{'account'};
            my $perm = $data->{'permission'};
            
            my $doc = Couchbase::Document->new(join(':', 'auth', $name, $perm));
            $cb->remove($doc);
        }
    }

    foreach my $auth (@{$atrace->{'authorization'}})
    {
        $receivers{$auth->{'actor'}} = 1;
    }
    
    foreach my $rcvr (keys %receivers)
    {
        my $docdata = {%{$tx}, %action_docdata};
        $docdata->{'receiver'} = $rcvr;
        push(@batch, Couchbase::Document->new
             (join(':', 'action', $docdata->{'block_num'}, $seq, $rcvr), $docdata));
        
    }
}



sub get_docs_pending
{
    my $ua = LWP::UserAgent->new();
    $ua->default_header('Authorization' => LWP::Authen::Basic->auth_header($dbuser, $dbpw) );
    my $res = $ua->get('http://' . $dbhost . ':9102/api/stats/histidx/action_01');
    if(not $res->is_success() )
    {
        die($res->decoded_content);
    }

    my $data = $json->decode($res->decoded_content);
    my $ret = $data->{'histidx:action_01'}{'num_docs_pending'};
    print STDERR ("Docs pending: $ret\n");
    return $ret;
}
    
        
sub check_indexer
{
    my $block_num = shift;

    if( $block_num > $last_checked_indexer + $ack_every * 10 )
    {
        if( get_docs_pending() > 100000 )
        {
            do
            {
                sleep(1);
            }
            while( get_docs_pending() > 10000 );
        }

        $last_checked_indexer = $block_num;
    }
}

   
