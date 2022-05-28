GAME_STATS_METHODS = ['collect_location_stats',
                      'run_stats_collector',
                      'collect_raid_stats',
                      'collect_mon_stats',
                      'collect_mon_iv_stats',
                      'collect_quest_stats']

class MITMMapperWrapper:
    def __init__(self, args, mitm_mapper):
        self._mitm_mapper = mitm_mapper
        self._setup_game_stats_methods(args.game_stats)

    def __getattr__(self, name):
        return getattr(self._mitm_mapper, name)

    def _setup_game_stats_methods(self, enabled):
        def _no_op(*args, **kwargs):
            return

        for method in GAME_STATS_METHODS:
            if enabled:
                setattr(self, method, getattr(self._mitm_mapper, method))
            else:
                setattr(self, method, _no_op)
