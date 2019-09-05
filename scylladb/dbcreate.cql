CREATE KEYSPACE IF NOT EXISTS eos
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};


USE eos;

CREATE TABLE actions
 (
 block_num         BIGINT,
 block_time        TIMESTAMP,
 trx_id            BLOB,
 global_action_seq BIGINT,
 action_ord        INT,
 crtr_action_ord   INT,
 contract          ASCII,
 action_name       ASCII,
 receiver          ASCII,
 PRIMARY KEY (block_num, global_action_seq, receiver)
 );


CREATE INDEX actions_trx_id ON actions (trx_id);

CREATE MATERIALIZED VIEW receiver_bloks AS
    SELECT receiver, block_num, block_time, trx_id,
           global_action_seq, action_ord, crtr_action_ord, contract, action_name
    FROM actions
    WHERE receiver IS NOT NULL AND global_action_seq IS NOT NULL AND block_num IS NOT NULL
    PRIMARY KEY (receiver, block_num, global_action_seq)
    WITH CLUSTERING ORDER BY (block_num DESC);





CREATE TABLE traces
 (
 block_num         BIGINT,
 trx_id            BLOB,
 jsdata            BLOB,
 PRIMARY KEY (block_num, trx_id)
 );

CREATE INDEX traces_trx_id ON traces (trx_id);


/* id=0: last written block,
   id=1: irreversible block,
   id=2: lowest block in traces,
   id=3: lowest block in actions */
CREATE TABLE pointers
 (
 id INT PRIMARY KEY,
 ptr BIGINT
 );
