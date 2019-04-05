CREATE KEYSPACE IF NOT EXISTS eosidx
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};


USE eosidx;

CREATE TABLE EOSIDX_ACTIONS
 (
 block_num         BIGINT,
 block_time        TIMESTAMP,
 trx_id            ASCII,
 global_action_seq BIGINT,
 parent            BIGINT,
 contract          ASCII,
 action_name       ASCII,
 receiver          ASCII,
 recv_sequence     BIGINT,
 PRIMARY KEY (block_num, global_action_seq)
 );



CREATE INDEX EOSIDX_ACTIONS_I01 ON EOSIDX_ACTIONS (trx_id);
CREATE INDEX EOSIDX_ACTIONS_I02 ON EOSIDX_ACTIONS (contract);
CREATE INDEX EOSIDX_ACTIONS_I03 ON EOSIDX_ACTIONS (receiver);

/* id=0: last written block, id=1: irreversible block */
CREATE TABLE EOSIDX_PTR
 (
 id INT PRIMARY KEY,
 ptr BIGINT
 );
