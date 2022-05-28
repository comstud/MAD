import sys
from multiprocessing.managers import SyncManager

import setproctitle

from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.db.PooledQueryExecutor import (PooledQueryExecutor,
                                              PooledQuerySyncManager)
from mapadroid.utils.logging import LoggerEnums, get_logger

logger = get_logger(LoggerEnums.database)


class DbFactory:
    @staticmethod
    def get_wrapper(args, multiproc=True, poolsize=None) -> (DbWrapper, SyncManager):
        if args.db_method == "monocle":
            logger.error(
                "MAD has dropped Monocle support. Please consider checking out the "
                "'migrate_to_rocketmap.sh' script in the scripts folder."
            )
            sys.exit(1)
        elif args.db_method != "rm":
            logger.error("Invalid db_method in config. Exiting")
            sys.exit(1)

        cls = PooledQueryExecutor
        db_pool_manager = None

        if multiproc:
            PooledQuerySyncManager.register("PooledQueryExecutor", cls)
            db_pool_manager = PooledQuerySyncManager()
            db_pool_manager.start(initializer=lambda: setproctitle.setproctitle('DbPool - %s' % setproctitle.getproctitle()))
            cls = db_pool_manager.PooledQueryExecutor

        if poolsize is None:
            poolsize = args.db_poolsize

        db_exec = cls(host=args.dbip, port=args.dbport,
                      username=args.dbusername, password=args.dbpassword,
                      database=args.dbname, poolsize=poolsize)
        db_wrapper = DbWrapper(db_exec=db_exec, args=args)

        return db_wrapper, db_pool_manager
