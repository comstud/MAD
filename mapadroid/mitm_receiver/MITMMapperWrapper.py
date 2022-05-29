from mapadroid import cache as redis

GAME_STATS_METHODS = ['collect_location_stats',
                      'run_stats_collector',
                      'collect_raid_stats',
                      'collect_mon_stats',
                      'collect_mon_iv_stats',
                      'collect_quest_stats']
PLAYERSTATS_KEY = 'playerstats:%s'

class MITMMapperWrapper:
    def __init__(self, args, mitm_mapper):
        self._args = args
        self._mitm_mapper = mitm_mapper
        self._setup_game_stats_methods(args.game_stats)
        self._player_stats_uses_redis = False

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

    def get_playerlevel(self, origin: str):
        if self._player_stats_uses_redis:
            cache = redis.get_cache(self._args, required=True)
            val = cache.hget(PLAYERSTATS_KEY % origin, 'level')
            if val is None:
                val = -1
            return int(val)
        return self._mitm_mapper.get_playerlevel(origin)

    def get_poke_stop_visits(self, origin: str) -> int:
        if self._player_stats_uses_redis:
            cache = redis.get_cache(self._args, required=True)
            val = cache.hget(PLAYERSTATS_KEY % origin, 'poke_stop_visits')
            if val is None:
                val = -1
            return int(val)
        return self._mitm_mapper.get_poke_stop_visits(origin)

    # Only proxy if we have player stats. And only proxy
    # the player stats, if so.
    def generate_player_stats(self, origin: str, data: dict):
        if 'inventory_delta' not in data:
            return
        stats = data['inventory_delta'].get("inventory_items", None)
        if stats is None or len(stats) == 0:
            return
        stat = None
        for data_inventory in stats:
            player_stats = data_inventory['inventory_item_data'].get('player_stats', {})
            player_level = player_stats.get('level', 0)
            if int(player_level) > 0:
                stat = data_inventory
        if stat is None:
            return
        if self._player_stats_uses_redis:
            cache = redis.get_cache(self._args, required=True)
            player_stats = stat['inventory_item_data']['player_stats']
            data = {
                'level': player_stats['level'],
                'experience': player_stats['experience'],
                'km_walked': player_stats['km_walked'],
                'pokemons_encountered': player_stats['pokemons_encountered'],
                'poke_stop_visits': player_stats['poke_stop_visits']}
            cache.hmset(PLAYERSTATS_KEY % origin, data)
        data = {'inventory_delta': {'inventory_items': [ stat ]}}
        return self._mitm_mapper.generate_player_stats(origin, data)
