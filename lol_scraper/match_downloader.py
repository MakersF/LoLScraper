import logging
import datetime
import random
import threading
import os
import time

from urllib.error import URLError

from cassiopeia import baseriotapi
from cassiopeia.dto.leagueapi import get_challenger, get_master
from cassiopeia.dto.matchlistapi import get_match_list
from cassiopeia.dto.matchapi import get_match
from cassiopeia.type.api.exception import APIError

from lol_scraper.data_types import Tier, Queue, Maps, unix_time, SimpleCache, cache_autostore
from lol_scraper.summoners_api import get_tier_from_participants, summoner_names_to_id

version_key = 'current_version'
delta_30_days = datetime.timedelta(days=30)
cache = SimpleCache()
LATEST = "latest"
max_analyzed_players_size = int(os.environ.get('MAX_ANALYZED_PLAYERS_SIZE', 50000))
EVICTION_RATE = float(os.environ.get('EVICTION_RATE', 0.5))  # Half of the analyzed players
players_download_threads = int(os.environ.get('PLAYERS_DOWNLOAD_THREADS', 2))
matches_download_threads = int(os.environ.get('MATCHES_DOWNLOAD_THREADS', 4))
logging_interval = int(os.environ.get('LOGGING_INTERVAL', 60))

patch_changed_lock = threading.Lock()
patch_changed = False


def do_every(seconds, func=None, *args, **kwargs):
    def g_tick():
        t = time.time()
        count = 0
        while True:
            count += 1
            yield max(t + count*seconds - time.time(),0)

    g = g_tick()
    while True:
        time.sleep(next(g))
        # This allows to use it in a for loop with a loop every 'seconds' seconds
        if func is None:
            yield
        else:
            func(*args, **kwargs)


class NoOpContextManager():
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FetchingException(Exception):

    def __init__(self, match):
        self.match = match

    def __repr__(self):
        return "Exception while fetching match {}".format(self.match)

    def __str__(self):
        return self.__repr__()


def riot_time(dt):
    if dt is None:
        dt = datetime.datetime.now()
    return int(unix_time(dt) * 1000)


def set_patch_changed(*args, **kwargs):
    with patch_changed_lock:
        global patch_changed
        patch_changed = True

def consume_path_changed():
    with patch_changed_lock:
        global patch_changed
        patch_changed = False

def get_patch_changed():
    with patch_changed_lock:
        global patch_changed
        return patch_changed


@cache_autostore(version_key, 60 * 60, cache, on_change=set_patch_changed)
def get_last_patch_version():
    version_extended = baseriotapi.get_versions()[0]
    version = ".".join(version_extended.split(".")[:2])
    logging.getLogger(__name__).info("Fetching version {}".format(version))
    return version


def check_minimum_patch(patch, minimum):
    if not minimum:
        return True
    if minimum.lower() != LATEST:
        return patch >= minimum
    else:
        try:
            version = get_last_patch_version()
            return patch >= version
        except:
            # in case the connection failed, do not store it, and try next round
            # Reject every version as we are not sure which is the latest version
            # and we don't want to pollute the data with patches with the wrong version
            return False


def handle_exception(e, logger):
    if isinstance(e, APIError):
        if 400 <= e.error_code < 500:
            # Might be a connection problem
            logger.warning("Encountered error {}".format(e))
        elif 500 <= e.error_code < 600:
            # Server problem. Let's give it some time
            logger.warning("Encountered error {}".format(e))
        else:
            logger.error("Encountered error {}".format(e))
    elif isinstance(e, URLError):
        logger.error("Encountered error {}. You are having connection issues".format(e))
    else:
        logger.exception("Encountered unexpected exception {}".format(e))


class PlayerDownloader(threading.Thread):

    def __init__(self, conf, players_to_analyze, analyzed_players, pta_lock, player_available_condition,
                 matches_to_download, mtd_lock, matches_available_condition,
                 logger, logger_lock):
        """

        :param dict conf:
        :param set players_to_analyze:
        :param set analyzed_players:
        :param threading.Lock pta_lock:
        :param threading.Condition player_available_condition:
        :param set matches_to_download:
        :param threading.Lock mtd_lock:
        :param threading.Condition matches_available_condition:
        :param logging.Logger logger:
        :param threading.Lock logger_lock:
        :return:
        """
        super(PlayerDownloader, self).__init__()
        self.conf = conf

        self.player_available_condition = player_available_condition
        self.matches_available_condition = matches_available_condition


        self.pta_lock = pta_lock
        self.players_to_analyze = players_to_analyze
        self.analyzed_players = analyzed_players

        self.mtd_lock = mtd_lock
        self.matches_to_download = matches_to_download

        self.logger_lock = logger_lock
        self.logger = logger

    def run(self):
        while not self.conf.get('exit', False):
            try:
                with self.pta_lock:
                    while not self.conf.get('exit', False):
                        try:
                            next_player = self.players_to_analyze.pop()
                            is_new = next_player not in self.analyzed_players
                            break
                        except KeyError:
                            self.player_available_condition.wait()
                            continue


                if is_new:
                    match_list = get_match_list(next_player, begin_time=riot_time(self.conf['start']),
                                                end_time=riot_time(self.conf['end']), ranked_queues=self.conf['queue'])
                    with self.mtd_lock:
                        self.matches_to_download.update(match.matchId for match in match_list.matches)
                        self.matches_available_condition.notify_all()
                    with self.pta_lock:
                        self.analyzed_players.add(next_player)
                        # analyzed_players grows indefinitely. This doesn't make sense, as after a while a player have
                        # new matches. When the list grows too big we remove a part of the players,
                        # so that they can be analyzed again.
                        if len(self.analyzed_players) > max_analyzed_players_size:
                            self.analyzed_players = {player_id for player_id in self.analyzed_players
                                                if random.random() < EVICTION_RATE}

            except Exception as e:
                with self.logger_lock:
                    handle_exception(e, self.logger)

    @property
    def total_downloads(self):
        with self.pta_lock:
            return len(self.analyzed_players)

class MatchDownloader(threading.Thread):

    def __init__(self, conf, players_to_analyze, pta_lock, player_available_condition,
                 matches_to_download, downloaded_matches, mtd_lock, matches_available_condition,
                 match_downloaded_callback, user_function_lock, logger, logger_lock):
        """

        :param dict conf:
        :param set players_to_analyze:
        :param threading.Lock pta_lock:
        :param threading.Condition player_available_condition:
        :param set matches_to_download:
        :param set downloaded_matches:
        :param threading.Lock mtd_lock:
        :param threading.Condition matches_available_condition:
        :param (dict, str) -> None match_downloaded_callback:
        :param threading.Lock user_function_lock:
        :param logging.Logger logger:
        :param threading.Lock logger_lock:
        :return:
        """
        super(MatchDownloader, self).__init__()
        self.conf = conf

        self.player_available_condition = player_available_condition
        self.matches_available_condition = matches_available_condition

        self.pta_lock = pta_lock
        self.players_to_analyze = players_to_analyze

        self.mtd_lock = mtd_lock
        self.matches_to_download = matches_to_download
        self.downloaded_matches = downloaded_matches

        self.user_function_lock = user_function_lock
        self.match_downloaded_callback = match_downloaded_callback

        self.logger_lock = logger_lock
        self.logger = logger

        self.last_patch_changed = 0


    def fetch_match(self, match_id):
        try:
            match = get_match(match_id, self.conf['include_timeline'])
            if match.mapId == Maps[self.conf['map_type']].value:
                match_min_tier, participant_tiers = get_tier_from_participants(match.participantIdentities,
                                                                               Tier.parse(self.conf['minimum_tier']),
                                                                               Queue[self.conf['queue']])

                valid = (match_min_tier.is_better_or_equal(Tier.parse(self.conf['minimum_tier']))
                        and check_minimum_patch(match.matchVersion, self.conf['minimum_patch']))
                return match, match_min_tier if valid else None, participant_tiers
        except Exception as e:
            raise FetchingException(match_id) from e

    def run(self):
        while not self.conf.get('exit', False):
            try:
                with self.mtd_lock:
                    while not self.conf.get('exit', False):
                        try:
                            next_match = self.matches_to_download.pop()
                            is_new = next_match not in self.downloaded_matches
                            break
                        except KeyError:
                            self.matches_available_condition.wait()
                            continue

                if is_new:
                    match, match_min_tier, participant_tiers = self.fetch_match(next_match)
                    with self.pta_lock:
                        for ids in participant_tiers.values():
                            self.players_to_analyze.update(ids)
                        self.player_available_condition.notify_all()

                    with self.mtd_lock:
                        self.downloaded_matches.add(next_match)

                    if match_min_tier:
                        with self.user_function_lock:
                            self.match_downloaded_callback(match, match_min_tier.name)

                    # When a new patch is released, we can clear all the downloaded_matches
                    # if minimum_patch == 'latest'
                    with self.mtd_lock:
                        if self.conf['minimum_patch'].lower() == LATEST and get_patch_changed():
                            self.downloaded_matches.clear()
                            consume_path_changed()
            except Exception as e:
                with self.logger_lock:
                    handle_exception(e, self.logger)

    @property
    def total_downloads(self):
        with self.mtd_lock:
            return len(self.downloaded_matches)


def download_matches(match_downloaded_callback, on_exit_callback, conf, synchronize_callback= True):
    """
    :param match_downloaded_callback:       function       when a match is downloaded function is called with the match
                                                            and the tier (league) of the lowest player in the match
                                                            as parameters

    :param on_exit_callback:                function        when this function is terminating on_exit_callback is called
                                                            with the remaining players to download, the downloaded
                                                            players, the id of the remaining matches to download and
                                                            the id of the downloaded matches

    :param conf:                            dict           a dictionary containing all the configuration parameters

    :param synchronize_callback:            bool            Synchronize the calls to match_downloaded_callback
                                                            If set to True the calls are wrapped by a lock, so that only
                                                            one at a time is executing

    :return:                                None
    """

    logger = logging.getLogger(__name__)
    if conf['logging_level'] != logging.NOTSET:
        logger.setLevel(conf['logging_level'])
    else:
        # possibly set the level to warning
        pass

    def checkpoint(players_to_analyze, analyzed_players, matches_to_download, downloaded_matches):
        logger.info("Reached the checkpoint."
                    .format(datetime.datetime.now().strftime("%m-%d %H:%M:%S"), len(downloaded_matches)))
        if on_exit_callback:
            on_exit_callback(players_to_analyze, analyzed_players, matches_to_download,
                             downloaded_matches)

    players_to_analyze = set(conf['seed_players_id'])
    downloaded_matches = set(conf['downloaded_matches'])
    logger.info("{} previously downloaded matches".format(len(downloaded_matches)))
    matches_to_download = set(conf['matches_to_download'])
    logger.info("{} matches to download".format(len(matches_to_download)))

    analyzed_players = set()
    pta_lock = threading.Lock()
    players_available_condition = threading.Condition(pta_lock)
    mtd_lock = threading.Lock()
    matches_Available_condition = threading.Condition(mtd_lock)
    user_function_lock = threading.Lock() if synchronize_callback else NoOpContextManager()
    logger_lock = threading.Lock()
    threads = []

    try:
        logger.info("Starting fetching..")
        for _ in range(players_download_threads):
            player_downloader = PlayerDownloader(conf, players_to_analyze, analyzed_players, pta_lock, players_available_condition,
                                                 matches_to_download , mtd_lock, matches_Available_condition,
                                                 logger, logger_lock)
            player_downloader.start()
            threads.append(player_downloader)

        for _ in range(matches_download_threads):
            match_downloader = MatchDownloader(conf, players_to_analyze, pta_lock, players_available_condition,
                                               matches_to_download, downloaded_matches, mtd_lock, matches_Available_condition,
                                               match_downloaded_callback, user_function_lock,
                                               logger, logger_lock)
            match_downloader.start()
            threads.append(match_downloader)

        for i, _ in enumerate(do_every(1)):
            if conf.get('exit', False):
                break
            # Execute every 60 seconds, but we want to pool the exit flag every second.
            if i % logging_interval != 0:
                continue
            with mtd_lock:
                matches_in_queue = len(matches_to_download)
                total_matches = len(downloaded_matches)
            with pta_lock:
                players_in_queue = len(players_to_analyze)
                total_players = len(analyzed_players)
            with logger_lock:
                logger.info("Players in queue: {}. Downloaded players: {}. Matches in queue: {}. Downloaded matches: {}"
                                .format(players_in_queue, total_players, matches_in_queue, total_matches))

        # Notify all the waiting threads to exit
        with pta_lock:
            players_available_condition.notify_all()
        with mtd_lock:
            matches_Available_condition.notify_all()
        logger.info("Terminating fetching")

    finally:
        # Joining threads before saving the state
        for thread in threads:
            thread.join()
        # Always call the checkpoint, so that we can resume the download in case of exceptions.
        logger.info("Calling checkpoint callback")
        checkpoint(players_to_analyze, analyzed_players, matches_to_download, downloaded_matches)


def prepare_config(config):
    runtime_config = {}

    runtime_config['logging_level'] = logging._nameToLevel[config.get('logging_level', 'NOTSET')]

    # Parse the time boundaries
    runtime_config['end'] = None if not 'end_time' in config else datetime.datetime(**config['end_time'])
    runtime_config['start'] = None if not 'start_time' in config else datetime.datetime(**config['start_time'])

    if runtime_config['start'] is None:
        runtime_config['start'] = (runtime_config['end'] if runtime_config[
            'end'] else datetime.datetime.now()) - delta_30_days

    runtime_config['minimum_patch'] = config.get('minimum_patch', "")
    runtime_config['queue'] = config.get('queue', Queue.RANKED_SOLO_5x5.name)
    runtime_config['map_type'] = config.get('map', Maps.SUMMONERS_RIFT.name)
    runtime_config['minimum_tier'] = config.get('minimum_tier', Tier.bronze.name).lower()

    runtime_config['include_timeline'] = config.get('include_timeline', True)

    runtime_config['downloaded_matches'] = config.get('downloaded_matches', ())

    runtime_config['matches_to_download'] = config.get('matches_to_download', ())

    runtime_config['seed_players_id'] = config.get('seed_players_id', None)

    if not runtime_config['seed_players_id']:
        while True:
            try:
                config_seed_players = config.get('seed_players', None)
                if config_seed_players is None:
                    # Let's use challenger and master tier players as seed
                    runtime_config['seed_players_id'] = (
                        list(league_entry_dto.playerOrTeamId for league_entry_dto in get_challenger(runtime_config['queue']).entries) +
                        list(league_entry_dto.playerOrTeamId for league_entry_dto in get_master(runtime_config['queue']).entries)
                    )
                else:
                    # We have a list of seed players. Let's use it
                    runtime_config['seed_players_id'] = list(summoner_names_to_id(config_seed_players).values())

                break
            except APIError:
                logger = logging.getLogger(__name__)
                logger.exception("APIError while initializing the script")
                # sometimes the network might have problems during the start. We don't want to crash just
                # because of that. Keep trying!

    return runtime_config


def setup_riot_api(conf):
    cassioepia = conf['cassiopeia']
    baseriotapi.set_api_key(cassioepia['api_key'])
    baseriotapi.set_region(cassioepia['region'])

    limits = cassioepia.get('rate_limits', None)
    if limits is not None:
        if isinstance(limits[0], (list, tuple)):
            baseriotapi.set_rate_limits(*limits)
        else:
            baseriotapi.set_rate_limit(limits[0], limits[1])

    baseriotapi.print_calls(cassioepia.get('print_calls', False))
