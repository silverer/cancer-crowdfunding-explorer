from src.scrapingtools.scrapemanager import ScrapeManager
from src.scrapingtools.urlcollectors import URLCollector, WaybackURLCollector
from src import data_io
from pathlib import Path

# SCRAPE ON BOTH GFM AND WAYBACK MACHINE
# create a table of all urls to scrape, only need to generate once
# tablepath, urls_df = URLCollector().create_url_table()
# wbcollector = WaybackURLCollector(start_year=2019,end_year=2021)
# wbtablepath, wburls_df = wbcollector.create_url_table()
# masterpath, master_df = wbcollector.compare_url_tables(urls_df, wburls_df)
# # start scraping
# manager = ScrapeManager(urltable_path=masterpath)
# manager.deploy()


tablepath = data_io.input_raw / "wayback_20210220" / "master_urls_table.csv"
tablecolumn = "cleaned_url"
savepath = data_io.input_raw / "scrape_output"
manager = ScrapeManager(
    urltable_path=tablepath, urltable_column=tablecolumn, savepath=savepath
)
manager.deploy(resume=True)


# RESUMING A PREVIOUS SCRAPE SESSION
# tablepath = data_io.input_raw / "wayback_20210220" / "master_urls_table.csv"
# tablecolumn = "cleaned_url"
# savepath = data_io.input_raw / "scrape_output"
# manager = ScrapeManager(
#     urltable_path=tablepath, urltable_column=tablecolumn, savepath=savepath
# )
# manager.deploy(resume=True)


# SCRAPE ONLY DIRECTLY FROM GFM DOMAIN @ GOFUNDME.COM
# dont scrape or retrieve urls from wayback
# create a table of all urls to scrape, only need to generate once
tablepath, urls_df = URLCollector().create_url_table()
# start scraping
manager = ScrapeManager(
    urltable_path=tablepath, urltable_column="url", use_wayback=False
)
manager.NEW_FILE_THRESH = 20
manager.deploy()

# RESUMING A PREVIOUS SCRAPE SESSION
tablepath = Path.cwd().parent / "sitemap_20200220" / "gfm_urls.csv"
tablecolumn = "url"
savepath = Path.cwd().parent / "scrape_output"
manager = ScrapeManager(
    urltable_path=tablepath, urltable_column=tablecolumn, savepath=savepath
)
manager.deploy(resume=True)
