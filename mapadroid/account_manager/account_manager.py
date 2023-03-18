from mapadroid import cache as redis
from mapadroid.db import DbFactory

class AccountManager:
    def __init__(self, application_args, db_wrapper=None):
        self._application_args = application_args
        self._encounter_limit = application_args.account_encounter_limit
        if db_wrapper is None:
            db_wrapper, _unused = DbFactory.DbFactory.get_wrapper(application_args, multiproc=False, poolsize=2)
        self._db_wrapper = db_wrapper

    def _get_cache(self):
        return redis.get_cache(self._application_args, required=True)

    def _encounters_key(self, login_type, username):
        return 'acct-encr:%s:%s' % (login_type, username)

    def _encounters_cache_key_for_account(self, acct):
        if acct is None:
            return None
        return self._encounters_key(acct['login_type'], acct['username'])

    def _get_current_account(self, origin):
        cache = self._get_cache()
        key = 'cur-acct:%s' % origin
        val = cache.get(key)
        if val is not None:
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            login_type, username = val.split(':', 1)
            encounters = cache.get(self._encounters_key(login_type, username))
            if encounters is None:
                encounters = 0
            else:
                encounters = int(encounters)
            return {
                'login_type': login_type,
                'username': username,
                'encounters': encounters}
        acct = self._db_wrapper.get_current_account_for_device(origin)
        if acct is None:
            return None
        p = cache.pipeline()
        p.set(key, '%s:%s' % (acct['login_type'], acct['username']))
        p.set(self._encounters_cache_key_for_account(acct), acct['encounters'])
        p.execute()
        return acct


    def add_encounters(self, origin, num=1):
        if not self._encounter_limit:
            return
        cache = self._get_cache()
        acct = self._get_current_account(origin)
        key = self._encounters_cache_key_for_account(acct)
        new_num = cache.incr(key, num)
        old_num = new_num - num
        if (old_num / 50) != (new_num / 50):
            self._db_wrapper.update_encounters_for_account(
                    acct['username'], acct['login_type'], new_num)
        return new_num

    def account_at_encounter_limit(self, origin):
        if not self._encounter_limit:
            return
        cache = self._get_cache()
        acct = self._get_current_account(origin)
        if not acct:
            return
        if acct['encounters'] >= self._encounter_limit:
            return acct['encounters']
        return

    def get_new_account_for_device(self, origin):
        if not self._encounter_limit:
            return
        acct = self._db_wrapper.get_new_account_for_device(origin)
        if acct is None:
            return
        cache = self._get_cache()
        acct_key = 'cur-acct:%s' % origin
        enc_key = self._encounters_cache_key_for_account(acct)
        p = cache.pipeline()
        p.set(acct_key, '%s:%s' % (acct['login_type'], acct['username']))
        p.set(enc_key, acct['encounters'])
        p.execute()
        return acct

    def set_account_banned(self, origin):
        if not self._encounter_limit:
            return
        acct = self._get_current_account(origin)
        if acct is None:
            return
        self._db_wrapper.set_account_banned(acct['username'], acct['login_type'], acct['encounters'])
        return
