. ./couchvars.sh

alias q="cbc-n1ql -u $COUCH_USER -P $COUCH_PW -U couchbase://localhost/$COUCH_BUCKET"


q "CREATE INDEX action_upd_01 ON $COUCH_BUCKET(TONUM(block_num) DESC) WHERE type = 'action_upd'"

q "CREATE INDEX action_upd_02 ON $COUCH_BUCKET(receiver, TONUM(block_num) DESC, TONUM(global_seq) DESC) WHERE type = 'action_upd'"

q "CREATE INDEX action_upd_03 ON $COUCH_BUCKET(trx_id) WHERE type = 'action_upd'"




q "CREATE INDEX action_01 ON $COUCH_BUCKET(TONUM(block_num) DESC) WHERE type = 'action'"

q "CREATE INDEX action_02 ON $COUCH_BUCKET(receiver, TONUM(block_num) DESC, TONUM(global_seq) DESC) WHERE type = 'action'"

q "CREATE INDEX action_03 ON $COUCH_BUCKET(trx_id) WHERE type = 'action'"





q "CREATE INDEX trace_upd_01 ON $COUCH_BUCKET(TONUM(block_num) DESC) WHERE type = 'trace_upd'"

q "CREATE INDEX trace_01 ON $COUCH_BUCKET(TONUM(block_num) DESC) WHERE type = 'trace'"


q "CREATE INDEX auth_01 ON $COUCH_BUCKET(account) WHERE type = 'auth'"

q "CREATE INDEX auth_02 ON $COUCH_BUCKET(DISTINCT ARRAY k.\`key\` FOR k IN auth.\`keys\` END) WHERE type = 'auth'"
