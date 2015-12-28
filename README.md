#LoLScraper

LoLScraper is a python script to download and store League of Legends matches with the Riot API.

##Simple
To build your matches dataset it is as simple as 

```python3 lol_scraper/main.py config.json```

with config.json
```json
{
  "cassiopeia": {
    "api_key": "your-api-key",
    "region": "NA"
  },
  "destination_directory": "path-to-directory",
}
```

##What It Does
LoLScraper will look for recent matches of some initial players. You can provide the initial players ( called seed players), otherwise the challenger and master league players of the region you selected are used.
Once it has a list of players, it downloads the match history of these players and downloads their matches.
If the matches satisfy the conditions you put in the configuration file they are stored.
Then, it adds the players which were in the stored match to its initial list of players and the process repeats.

##Efficient
LoLScraper will

 - store the matches as compressed files
 - sleep while waiting for the rate limits, not consuming CPU time
 - avoids pulling matches only from a few players
 - only downloads a match once ( no repetitions )

##Configurable
While the example configuration is extremely short and easy to use, the available options cover all the needs. 

[This comprehensive configuration](https://github.com/MakersF/LoLScraper/blob/master/riot_scraper/configuration%5Bno%20annotations%5D.json) shows all the options available.
[This is the same configuration](https://github.com/MakersF/LoLScraper/blob/master/riot_scraper/configuration.json) with several annotations to specify what element is optional and a brief description of them.

If the `destination_directory` element starts with `__file__`, `__file__` will be replaced with the directory containing the configuration json. This way you can specify a directory relative to the configuration file.

##Customizable
If your needs are different from the usual ones, you can import LoLScraper as a library.
The [`download_matches` function](https://github.com/MakersF/LoLScraper/blob/master/riot_scraper/match_downloader.py) takes a `store_callback` function in addition to the configuration parameters. The callback is called every time a match is downloaded. You can pass your own function and do whatever you want with the stored matches: send it over ssh to another server, translate it to Klingon, restructure it to XML, remove the parts you know you wont use, or just ignore it.
If you need more customization in setting the seed players you can use the `seed_players_by_tier` key in the configuration file.

To stop the fetching, set the key `exit` to `True` in the configuration dictionary you passed to the method.

##Setup
If you want to use LolScraper as a library, you can install it with
`pip install lol_scraper`
If you want to use it as a script, there is no need to install it. Just download the repository, and call
`python3 lol_scraper/main.py configuration_file.json`

##Dependencies
LoLScraper is build over [Cassiopeia](https://github.com/robrua/cassiopeia).
If you are installing with `pip` it is installed automatically.
If you are manually downloading the repository you need to install cassiopeia.
You can install Cassiopeia following the [library setup documentation](https://github.com/robrua/cassiopeia#setup).
Here is a snippet (or better, all it takes)
``` pip3 install cassiopeia```

If you prefer to not install Cassiopeia, you have an alternative!
[Download the Cassiopeia repository](https://github.com/robrua/cassiopeia/archive/master.zip), extract it, set the path into which you extracted them into [the scripts](https://github.com/MakersF/LoLScraper/tree/master/riot_scraper/run_scripts), and call `run_scripts/match_downloader` from your CLI as if you were calling `lol_scraper/main.py`

##Tests
The tests require an API key. Create a file called ```api-key``` in the project root directory (where the .gitignore file is stored) with only your api key inside. The file is already on .gitignore, so there is no risk for you to commit and push it on the web.

##Disclaimer
LoLScraper isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing League of Legends. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.
