CREATE DATABASE eosidx;

CREATE USER 'eosidx'@'localhost' IDENTIFIED BY 'guugh3Ei';
GRANT ALL ON eosidx.* TO 'eosidx'@'localhost';
grant SELECT on eosidx.* to 'eosidxro'@'%' identified by 'eosidxro';

use eosidx;



CREATE TABLE EOSIDX_TRX
(
 trx_seq           BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
 trx_id            VARCHAR(64) NOT NULL,
 block_time        DATETIME NOT NULL,
 block_num         BIGINT NOT NULL,
 irreversible      TINYINT NOT NULL DEFAULT 0
 ) ENGINE=InnoDB;
 
CREATE INDEX EOSIDX_TRX_I01 ON EOSIDX_TRX (block_num);
CREATE INDEX EOSIDX_TRX_I02 ON EOSIDX_TRX (block_time);
CREATE INDEX EOSIDX_TRX_I03 ON EOSIDX_TRX (trx_id(8));
CREATE INDEX EOSIDX_TRX_I04 ON EOSIDX_TRX (irreversible, block_num);


CREATE TABLE EOSIDX_ACTIONS
 (
 global_action_seq BIGINT UNSIGNED PRIMARY KEY,
 parent            BIGINT UNSIGNED NOT NULL,
 trx_seq_num       BIGINT UNSIGNED NOT NULL,
 contract          VARCHAR(13) NOT NULL,
 action_name       VARCHAR(13) NOT NULL,
 receiver          VARCHAR(13) NOT NULL,
 recv_sequence     BIGINT UNSIGNED NOT NULL,
 FOREIGN KEY (trx_seq_num)
   REFERENCES EOSIDX_TRX(trx_seq)
   ON DELETE CASCADE
 ) ENGINE=InnoDB;

CREATE INDEX EOSIDX_ACTIONS_I01 ON EOSIDX_ACTIONS (parent);
CREATE INDEX EOSIDX_ACTIONS_I03 ON EOSIDX_ACTIONS (contract, action_name, trx_seq_num);
CREATE INDEX EOSIDX_ACTIONS_I04 ON EOSIDX_ACTIONS (receiver, contract, action_name, trx_seq_num);
CREATE INDEX EOSIDX_ACTIONS_I05 ON EOSIDX_ACTIONS (receiver, recv_sequence);
