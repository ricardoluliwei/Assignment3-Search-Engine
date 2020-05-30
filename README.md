TO CREATE INVERT INDEX FOLDER
-------------------------
Run 

CONFIGURATION
-------------------------

### Step 1: To create invert index folder
Using 

### Step 2: Run search operation

```
class Response:
    Attributes:
        url:
            The URL identifying the response.
        status:
            An integer that identifies the status of the response. This
            follows the same status codes of http.
            (REF: https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html)
            In addition there are status codes provided by the caching
            server (600-606) that define caching specific errors.
        error:
            If the status codes are between 600 and 606, the reason for
            the error is provided in this attrbiute. Note that for status codes
            (400-599), the error message is not put in this error attribute; instead it
            must picked up from the raw_response (if any, and if useful).
        raw_response:
            If the status is between 200-599 (standard http), the raw
            response object is the one defined by the requests library.
            Useful resources in understanding this raw response object:
                https://realpython.com/python-requests/#the-response
                https://requests.kennethreitz.org/en/master/api/#requests.Response
            HINT: raw_response.content gives you the webpage html content.
```
**Return Value**

This function needs to return a list of urls that are scraped from the
response. (An empty list for responses that are empty). These urls will be
added to the Frontier and retrieved from the cache. These urls have to be
filtered so that urls that do not have to be downloaded are not added to the
frontier.

The first step of filtering the urls can be by using the **is_valid** function
provided in the same scraper.py file. Additional rules should be added to the is_valid function to filter the urls.

EXECUTION
-------------------------

To execute the crawler run the launch.py command.
```python3 launch.py```

You can restart the crawler from the seed url
(all current progress will be deleted) using the command
```python3 launch.py --restart```

You can specifiy a different config file to use by using the command with the option
```python3 launch.py --config_file path/to/config```

ARCHITECTURE
-------------------------

### FLOW

The crawler receives a cache host and port from the spacetime servers
and instantiates the config.

It launches a crawler (defined in crawler/\_\_init\_\_.py L5) which creates a 
Frontier and Worker(s) using the optional parameters frontier_factory, and
worker_factory.

When the crawler in started, workers are created that pick up an
undownloaded link from the frontier, download it from our cache server, and
pass the response to your scraper function. The links that are received by
the scraper is added to the list of undownloaded links in the frontier and
the url that was downloaded is marked as complete. The cycle continues until
there are no more urls to be downloaded in the frontier.

### REDEFINING THE FRONTIER:

You can make your own frontier to use with the crawler if they meet this
interface definition:
```
class Frontier:
    def __init__(self, config, restart):
        #Initializer.
        # config -> Config object (defined in utils/config.py L1)
        #           Note that the cache server is already defined at this
        #           point.
        # restart -> A bool that is True if the crawler has to restart
        #           from the seed url and delete any current progress.

    def get_tbd_url(self):
        # Get one url that has to be downloaded.
        # Can return None to signify the end of crawling.

    def add_url(self, url):
        # Adds one url to the frontier to be downloaded later.
        # Checks can be made to prevent downloading duplicates.
    
    def mark_url_complete(self, url):
        # mark a url as completed so that on restart, this url is not
        # downloaded again.
```
A sample reference is given in utils/frontier.py L10. Note that this
reference is not thread safe.

### REDEFINING THE WORKER

You can make your own worker to use with the crawler if they meet this
interface definition:
```
from scraper import scraper
from utils.download import download
class Worker(Thread): # Worker must inherit from Thread or Process.
    def __init__(self, worker_id, config, frontier):
        # worker_id -> a unique id for the worker to self identify.
        # config -> Config object (defined in utils/config.py L1)
        #           Note that the cache server is already defined at this
        #           point.
        # frontier -> Frontier object created by the Crawler. Base reference
        #           is shown in utils/frontier.py L10 but can be overloaded
        #           as detailed above.
        self.config = config
        super().__init__(daemon=True)

    def run(self):
        In loop:
            > url = get one undownloaded link from frontier.
            > resp = download(url, self.config)
            > next_links = scraper(url, resp)
            > add next_links to frontier
            > sleep for self.config.time_delay
```
A sample reference is given in utils/worker.py L9.

THINGS TO KEEP IN MIND
-------------------------

1. It is important to filter out urls that do not point to a webpage. For
   example, PDFs, PPTs, css, js, etc. The is_valid filters a large number of
   such extensions, but there may be more.
2. It is important to filter out urls that are not with ics.uci.edu domain.
3. It is important to maintain the politeness to the cache server (on a per
   domain basis).
4. It is important to set the user agent in the config.ini correctly to get
   credit for hitting the cache servers.
5. Launching multiple instances of the crawler will download the same urls in
   both. Mecahnisms can be used to avoid that, however the politeness limits
   still apply and will be checked.
6. Do not attempt to download the links directly from ics servers.
