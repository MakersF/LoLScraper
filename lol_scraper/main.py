import os
import argparse
import logging
import pickle
from json import loads
from contextlib import closing, suppress

from lol_scraper.persist import TierStore
from lol_scraper.match_downloader import setup_riot_api, prepare_config, download_matches
from lol_scraper.data_types import Tier, TierSeed, TierSet

current_state_extension = '.pickle'


def make_store_callback(store):
    def store_callback(match, tier):
        store.store(match.to_json(sort_keys=False,indent=None), tier)
    return store_callback


def download_from_config(conf, store_callback, checkpoint_callback):
    setup_riot_api(conf)
    runtime_config = prepare_config(conf)

    download_matches(store_callback, checkpoint_callback, runtime_config)


def time_slice_end_callback(config_file, players_to_analyze, analyzed_players, matches_to_download_by_tier, downloaded_matches, total_matches):
    with open(config_file + current_state_extension, mode='wb') as matches:
        pickle.dump((players_to_analyze.to_json(), matches_to_download_by_tier.to_json(), downloaded_matches), matches)


def load_players_and_matches_ids_into(config_file, conf):
    with suppress(FileNotFoundError), open(config_file + current_state_extension, mode='rb') as matches:
        players_to_analyse, matches_to_download_by_tier, downloaded_matches = pickle.load(matches)
        conf['seed_players_by_tier'] = TierSeed().from_json(players_to_analyse)
        conf['matches_to_download_by_tier'] = TierSet(max_items_per_set=2000).from_json(matches_to_download_by_tier)
        conf['downloaded_matches'] = downloaded_matches


def main(configuration_file, no_state=False):
    with open(configuration_file, 'rt') as config_file:
        json_conf = loads(config_file.read())

    load_players_and_matches_ids_into(configuration_file, json_conf)

    base_file_name = json_conf.get('base_file_name', '')
    matches_per_file = json_conf.get('matches_per_file', 0)
    destination_directory = json_conf['destination_directory']
    # Allow the directory to be relative to the config file.
    if destination_directory.startswith('__file__'):
        configuration_file_dir = os.path.dirname(os.path.realpath(configuration_file))
        destination_directory=destination_directory.replace('__file__', configuration_file_dir)

    checkpoint_callback = lambda *args, **kwargs: time_slice_end_callback(configuration_file, *args, **kwargs) if not no_state else None

    with closing(TierStore(destination_directory, matches_per_file, base_file_name)) as store:
        download_from_config(json_conf, make_store_callback(store), checkpoint_callback)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('configuration_file',help='The json file to hold the configuration of the download session '
                                                  'you want to start by running this script. Might be a file saved '
                                                  'from a previous session',action='store')
    parser.add_argument('--no-state', action='store_true', help='Do not store in a .state file the current state of '
                                                           'execution, so that if the process is stopped it can be '
                                                           'resumed from the last state saved',
                        default=False)
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s, %(levelname)s, %(name)s, %(message)s',
                        datefmt="%m-%d %H:%M:%S",
                        level=logging.INFO)

    main(args.configuration_file, args.no_state)