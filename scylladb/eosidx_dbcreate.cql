CREATE KEYSPACE IF NOT EXISTS eosidx
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};


USE eosidx;

CREATE TABLE actions
 (
 block_num         BIGINT,
 block_time        TIMESTAMP,
 trx_id            ASCII,
 global_action_seq BIGINT,
 parent            BIGINT,
 contract          ASCII,
 action_name       ASCII,
 receiver          ASCII,
 PRIMARY KEY (block_num, global_action_seq, receiver)
 );


CREATE INDEX actions_trx_id ON actions (trx_id);

CREATE MATERIALIZED VIEW receiver_bloks AS
    SELECT receiver, block_num, block_time, trx_id, global_action_seq, parent, contract, action_name
    FROM actions
    WHERE receiver IS NOT NULL AND global_action_seq IS NOT NULL AND block_num IS NOT NULL
    PRIMARY KEY (receiver, block_num, global_action_seq)
    WITH CLUSTERING ORDER BY (block_num DESC);




/* id=0: last written block, id=1: irreversible block */
CREATE TABLE pointers
 (
 id INT PRIMARY KEY,
 ptr BIGINT
 );
