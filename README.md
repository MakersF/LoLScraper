#LoLScraper

LoLScraper is a python script to download and store League of Legends matches with the Riot API.

##Simple
To build your matches dataset it is as simple as 

```python3 match_downloader.py config.json```

with config.json
```json
{
  "cassiopeia": {
    "api_key": "your-api-key",
    "region": "NA"
  },
  "destination_directory": "path-to-directory",
  "seed_players": ["nightblue3", "feed l0rd"]
}
```

LoLScraper will look for recent matches of the seed players and start downloading and storing them. Then it will repeat the process for every player who played in those matches.

##Efficient
LoLScraper will

 - store the matches as compressed files
 - sleep while waiting for the rate limits, not consuming CPU time
 - avoids pulling matches only from a few players

##Configurable
While the example configuration is extremely short and easy to use, the available options cover all the needs. 

[This comprehensive configuration](https://github.com/MakersF/LoLScraper/blob/master/riot_scraper/configuration%5Bno%20annotations%5D.json) shows all the options available.
[This is the same configuration](https://github.com/MakersF/LoLScraper/blob/master/riot_scraper/configuration.json) with several annotations to specify what element is optional and a brief description of them.

If the `destination_directory` element starts with `__file__`, `__file__` will be replaced with the directory containing the configuration json. This way you can specify a directory relative to the configuration file.

##Customizable
If your needs are different from the usual ones, you can import LoLScraper as a library.
The [`download_matches` function](https://github.com/MakersF/LoLScraper/blob/master/riot_scraper/match_downloader.py) takes a `store_callback` function in addition to the configuration parameters file exposes. The callback is called every time a match is downloaded. You can pass your own function and do whatever you want with the stored matches: send it over ssh to another server, translate it to Klingon, restructure it to XML, remove the parts you know you wont use, or just ignore it. 

##Setup
No need to install it. Just download the repository, and call
`python3 match_downloader.py configuration_file.json`

##Dependencies
LoLScraper is build over [Cassiopeia](https://github.com/robrua/cassiopeia).
You can install Cassiopeia following the [library setup documentation](https://github.com/robrua/cassiopeia#setup).
Here is a snippet (or better, all it takes)
``` pip3 install cassiopeia```

If you prefer to not install Cassiopeia, you have an alternative!
[Download the Cassiopeia repository](https://github.com/robrua/cassiopeia/archive/master.zip), extract it, set the path into which you extracted them into [the scripts](https://github.com/MakersF/LoLScraper/tree/master/riot_scraper/run_scripts), and call `match_downloader` from your CLI as if you were calling `match_downloader.py`

##Tests
The tests require an API key. Create a file called ```api-key``` in the project root directory (where the .gitignore file is stored) with only your api key inside. The file is already on .gitignore, so there is no risk for you to commit and push it on the web.

##Disclaimer
LoLScraper isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing League of Legends. League of Legends and Riot Games are trademarks or registered trademarks of Riot Games, Inc. League of Legends © Riot Games, Inc.
