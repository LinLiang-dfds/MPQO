EXTENSION = pg_bao
MODULE_big = pg_bao
DATA = pg_bao--0.0.1.sql
OBJS = main.o 
PG_CONFIG = /data1/linliang/postgresql12/pgsql/bin/pg_config
#PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
#SHLIB_LINK = -lsqlite3
include $(PGXS)
