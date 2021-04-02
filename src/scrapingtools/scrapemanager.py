import datetime
import math
import re
from pathlib import Path
import csv
import warnings
import urllib.parse as up
from collections import OrderedDict

import pandas as pd
from bs4 import BeautifulSoup

from .utils import log_message as print
from . import utils as utils
from . import waybackinterface as wbi
from . import scrapers as scrapers
from . import seleniumcontainer as renderercontainer

from .. import data_io

# defaults
OUTPATH = data_io.input_raw / "scrape_output"
URLTABLE_PATH = data_io.input_raw / "wayback_20200219" / "master_urls_table.csv"
URLTABLE_COLUMN = "cleaned_url"
URLTABLE_COLUMN2 = "original_url"
MAX_NUMBER_OF_SNAPSHOTS = 30
CAMPAIGNID_COLUMN = "cleaned_campaign_id"
CAMPAIGNID_COLUMN2 = "campaign_id"
SCRAPE_QUALITY_THRESHOLD = 17
PRESENT_SCRAPE_QUALITY_THRESHOLD = 20


class ScrapeManager(object):
    def __init__(
        self,
        urltable_path=URLTABLE_PATH,
        urltable_column=URLTABLE_COLUMN,
        urltable_column2=URLTABLE_COLUMN2,
        campaignid_column=CAMPAIGNID_COLUMN,
        campaignid_column2=CAMPAIGNID_COLUMN2,
        savepath=OUTPATH,
        use_wayback=True,
    ):
        self.savepath = savepath
        self.urltable_path = urltable_path
        self.URLTABLE_COLUMN = urltable_column
        self.URLTABLE_COLUMN2 = urltable_column2
        self.CAMPAIGNID_COLUMN = campaignid_column
        self.CAMPAIGNID_COLUMN2 = campaignid_column2
        self.SCRAPE_QUALITY_THRESHOLD = SCRAPE_QUALITY_THRESHOLD
        self.PRESENT_SCRAPE_QUALITY_THRESHOLD = PRESENT_SCRAPE_QUALITY_THRESHOLD
        print("Loaded {}".format(urltable_path.stem))
        self.NEW_FILE_THRESH = 10000
        self.RENDER_CONTAINER = renderercontainer.RendererContainer()
        print("Initialized render container")
        self.use_wayback = use_wayback
        self.MAX_NUMBER_OF_SNAPSHOTS = MAX_NUMBER_OF_SNAPSHOTS

    @property
    def savepath(self):
        return self._savepath

    @savepath.setter
    def savepath(self, spath):
        spath = utils.verify_pathtype(spath)
        if not spath.exists():
            warnings.warn("{} not exists, will create directory".format(spath))
            if not spath.parent.exists():
                raise ValueError(
                    "{} also does not exist, please double check value".format(
                        spath.parent
                    )
                )
            spath.mkdir()
        self._savepath = spath

    @property
    def URLTABLE_COLUMN(self):
        return self._URLTABLE_COLUMN

    @URLTABLE_COLUMN.setter
    def URLTABLE_COLUMN(self, col):
        if col in self.URLTABLE.columns:
            self._URLTABLE_COLUMN = col
        else:
            raise ValueError(
                "{} is not a column in the table of {}".format(col, self.urltable_path)
            )

    @property
    def URLTABLE_COLUMN2(self):
        return self._URLTABLE_COLUMN2

    @URLTABLE_COLUMN2.setter
    def URLTABLE_COLUMN2(self, col):
        if col in self.URLTABLE.columns:
            self._URLTABLE_COLUMN2 = col
        else:
            self._URLTABLE_COLUMN2 = None

    @property
    def CAMPAIGNID_COLUMN2(self):
        return self._CAMPAIGNID_COLUMN2

    @CAMPAIGNID_COLUMN2.setter
    def CAMPAIGNID_COLUMN2(self, col):
        if col in self.URLTABLE.columns:
            self._CAMPAIGNID_COLUMN2 = col
        else:
            self._CAMPAIGNID_COLUMN2 = None

    @property
    def CAMPAIGNID_COLUMN(self):
        return self._CAMPAIGNID_COLUMN

    @CAMPAIGNID_COLUMN.setter
    def CAMPAIGNID_COLUMN(self, col):
        if col in self.URLTABLE.columns:
            self._CAMPAIGNID_COLUMN = col
        else:
            self._CAMPAIGNID_COLUMN = None

    @property
    def urltable_path(self):
        return self._urltable_path

    @urltable_path.setter
    def urltable_path(self, urltable_path):
        urltable_path = utils.verify_pathtype(urltable_path)
        if not urltable_path.exists():
            raise ValueError("{} does not exist, recheck path".format(urltable_path))
        elif not urltable_path.is_file():
            raise ValueError("{} is not a file".format(urltable_path))
        self.URLTABLE = pd.read_csv(urltable_path, encoding="utf-8", dtype=str)
        self._urltable_path = urltable_path

    def output_filename_template(self, ii_num):
        # can change template but gotta leave ii_num in
        return f"master_scraped_output_i{ii_num}.csv"

    def _most_recent_ifile_regex_pattern(self):
        return self.output_filename_template("(\d+)")

    def _most_recent_ifile_star_pattern(self):
        return self.output_filename_template("*")

    def _wayback_scrape(self, url_to_scrape):
        msg = "wayback: none"
        #campaign_page = wbi.get_campaign_page(url_to_scrape)
        campaign_page = self.RENDER_CONTAINER.render(url_to_scrape)
        if campaign_page is None:
            msg = "wayback: request failed"
            row = scrapers.empty_url_row(url_to_scrape)
            return row, msg

        soup = BeautifulSoup(campaign_page.text, features="lxml")

        campaign_url = campaign_page.url

        result_dict = {}
        result_dict["2012"] = scrapers.scrape_url_2012(soup, campaign_url)
        result_dict["2014"] = scrapers.scrape_url_2014(soup, campaign_url)
        result_dict["2015"] = scrapers.scrape_url_2015(soup, campaign_url)
        result_dict["2018"] = scrapers.scrape_url_2018(soup, campaign_url)
        result_dict["2017"] = scrapers.scrape_url_2017(soup, campaign_url)
        result_dict["2019"] = scrapers.scrape_url_2019(soup, campaign_url)

        result_quality = {k: wbi.scrape_quality(v) for k, v in result_dict.items()}
        best_scraper_year = pd.Series(result_quality).idxmax()
        best_scraper_quality = result_quality[best_scraper_year]
        best_result = result_dict[best_scraper_year]

        if scrapers.is_inactive(soup):
            msg = "wayback: inactive"
        elif scrapers.not_found(soup):
            msg = "wayback: campaign not found"
        elif best_scraper_quality >= self.SCRAPE_QUALITY_THRESHOLD:
            msg = "wayback: success"
        else:
            msg = "wayback: scraped but did not meet success standard"
        print(msg)
        return best_result, msg

    def wayback_search_and_scrape(self, urls_to_search):
        try:
            # Seach for snapshots on Wayback
            result_list = []
            for url_to_search in urls_to_search:
                search_results = wbi.search_wayback(url_to_search)
                if search_results.empty:
                    continue
                parent_urls = wbi.clean_wayback_search_results(search_results)
                result_list.append(parent_urls)

            if len(result_list) == 0:
                msg = "wayback: url not found in archives"
                print(msg)
                row = scrapers.empty_url_row(url_to_search)
                row["archive_timestamp"] = "nat"
                row["query_url"] = url_to_search
                row["gfm_url"] = url_to_search
                row["wayback_status"] = msg
            else:
                search_results_all = pd.concat(
                    result_list, ignore_index=True, sort=False
                )
                if search_results_all.empty:
                    msg = "wayback: no archives found"
                    print(msg)
                    row = scrapers.empty_url_row(url_to_search)
                    row["archive_timestamp"] = "nat"
                    row["query_url"] = url_to_search
                    row["gfm_url"] = url_to_search
                    row["wayback_status"] = msg
                else:
                    search_results_all = search_results_all.sort_values(
                        "timestamp", ascending=False
                    )
                    # iter thru most recent result first
                    result_d = {}
                    for niter, (iloc, sresult) in enumerate(
                        search_results_all.iterrows()
                    ):
                        gfm_url = sresult.original
                        timestamp_to_scrape = sresult.timestamp
                        print(f"Getting archive on {timestamp_to_scrape}")
                        url_to_scrape = (
                            "http://web.archive.org/web/"
                            + f"{timestamp_to_scrape}/{gfm_url}"
                        )
                        row, msg = self._wayback_scrape(url_to_scrape)
                        result_d[niter] = (row, msg)
                        if msg == "wayback: success":
                            break
                        if niter > self.MAX_NUMBER_OF_SNAPSHOTS:
                            break
                        # ran through MAX_NUMBER_OF_SNAPSHOTS already and still no success
                        # most likely due to the URL being an internal
                        # gfm direct that is not a campaign page
                    row, msg = self.process_wayback_scrape_result(result_d)
                    print(msg)
                    row["archive_timestamp"] = timestamp_to_scrape
                    row["query_url"] = url_to_scrape
                    row["gfm_url"] = gfm_url
                    row["wayback_status"] = msg

        except Exception as e:
            row = scrapers.empty_url_row(url_to_search, mgs=str(e))
            row["archive_timestamp"] = "nat"
            row["query_url"] = url_to_search
            row["gfm_url"] = url_to_search
            row["wayback_status"] = "wayback: failed completely"
        return row

    def process_wayback_scrape_result(self, result_d):
        result_df = pd.DataFrame.from_dict(
            result_d, columns=["row", "msg"], orient="index"
        )
        if (result_df.msg == "wayback: success").any():
            # choose first most waybacksuccess
            out_res = result_df[result_df.msg == "wayback: success"].iloc[0, :]
            row, msg = out_res.row, out_res.msg
        elif (result_df.msg == "wayback: inactive").any():
            out_res = result_df[result_df.msg == "wayback: inactive"].iloc[0, :]
            row, msg = out_res.row, out_res.msg
        elif (
            result_df.msg == "wayback: scraped but did not meet success standard"
        ).any():
            out_res = result_df[
                result_df.msg == "wayback: scraped but did not meet success standard"
            ].iloc[0, :]
            row, msg = out_res.row, out_res.msg
        else:
            out_res = result_df.iloc[0, :]
            row, msg = out_res.row, out_res.msg
        return row, msg

    def present_scrape_url(self, urls_to_search):
        current_dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        scrape_sucess = False
        for url_to_search in urls_to_search:
            err_msg = "present: none"
            print(f"Present scraping: {url_to_search}")
            # campaign_page = wbi.get_campaign_page_accept_non_working(url_to_search).text
            campaign_page = self.RENDER_CONTAINER.render(url_to_search)
            if campaign_page is None:
                err_msg = "present: request failed"
                print(err_msg)
                row = scrapers.empty_url_row(url_to_search, msg=err_msg)
                continue
            soup = BeautifulSoup(campaign_page.text, features="lxml")

            campaign_url = campaign_page.url
            result_dict = {}
            result_dict["2018"] = scrapers.scrape_url_2018(soup, campaign_url)
            result_dict["2019"] = scrapers.scrape_url_2019(soup, campaign_url)
            result_quality = {k: wbi.scrape_quality(v) for k, v in result_dict.items()}
            best_scraper_year = pd.Series(result_quality).idxmax()
            best_scraper_quality = result_quality[best_scraper_year]
            row = result_dict[best_scraper_year]

            if scrapers.is_inactive(soup):
                err_msg = "present: inactive"
                print(err_msg)
            elif scrapers.not_found(soup):
                err_msg = "present: campaign not found"
                print(err_msg)
            elif best_scraper_quality >= self.PRESENT_SCRAPE_QUALITY_THRESHOLD:
                err_msg = "present: success"
                scrape_sucess = True
                print(err_msg)
                break

        if not scrape_sucess:
            if err_msg == "present: none":
                err_msg = "present: scraped but did not meet success criteria"
                print(err_msg)

        row["archive_timestamp"] = current_dt
        row["query_url"] = url_to_search
        row["gfm_url"] = url_to_search
        row["wayback_status"] = err_msg

        return row, scrape_sucess

    def _create_new_outfile(self, save_fullpath):
        print(f"creating new outfile {save_fullpath}")
        outfile = save_fullpath.open(mode="w", encoding="utf-8", newline="")
        writer = csv.writer(outfile)
        return writer, outfile

    def _make_save_fullpath(self, index):
        ii_num = (index // self.NEW_FILE_THRESH) * self.NEW_FILE_THRESH
        return self.savepath / self.output_filename_template(ii_num)

    def _load_outfile(self, save_fullpath):
        # loads an existing filepath
        print(f"loading outfile {save_fullpath}")
        outfile = save_fullpath.open(mode="a", encoding="utf-8", newline="")
        writer = csv.writer(outfile)
        return writer, outfile

    def _get_save_fullpath(self, index):
        # returns an existing filepath
        ii_num = int(math.floor(index / self.NEW_FILE_THRESH) * self.NEW_FILE_THRESH)
        return self.savepath / self.output_filename_template(ii_num)

    def _get_last_url_from_most_recent_ifile(self):
        # path of most recent ifile, sort by largest i number
        ifilepath = sorted(
            [*self.savepath.glob(self._most_recent_ifile_star_pattern())],
            key=lambda x: int(
                re.findall(self._most_recent_ifile_regex_pattern(), x.stem + x.suffix)[
                    0
                ]
            ),
        )[-1]
        ifile_df = pd.read_csv(ifilepath, encoding="utf-8")
        last_url = ifile_df.iloc[-1, :].gfm_url
        return last_url

    def start_from_latest_ifile(self):
        last_scraped_url = self._get_last_url_from_most_recent_ifile().lower()
        last_campaign_id = wbi.extract_campaign_id_from_gfm_url(
            last_scraped_url
        ).lower()
        url_index_d = OrderedDict()
        if self.CAMPAIGNID_COLUMN:
            last_scraped_url_index = self.URLTABLE.index[
                self.URLTABLE[self.CAMPAIGNID_COLUMN].str.lower() == last_campaign_id
            ].values
            url_index_d["CAMPAIGNID_COLUMN"] = last_scraped_url_index
        if self.CAMPAIGNID_COLUMN2:
            last_scraped_url_index = self.URLTABLE.index[
                self.URLTABLE[self.CAMPAIGNID_COLUMN2].str.lower() == last_campaign_id
            ].values
            url_index_d["CAMPAIGNID_COLUMN2"] = last_scraped_url_index
        if self.URLTABLE_COLUMN2:
            last_scraped_url_index = self.URLTABLE.index[
                self.URLTABLE[self.URLTABLE_COLUMN2].str.lower() == last_scraped_url
            ].values
            url_index_d["URLTABLE_COLUMN2"] = last_scraped_url_index
        last_scraped_url_index = self.URLTABLE.index[
            self.URLTABLE[self.URLTABLE_COLUMN].str.lower() == last_scraped_url
        ].values
        url_index_d["URLTABLE_COLUMN"] = last_scraped_url_index
        last_scraped_url_indices = [v[0] for k, v in url_index_d.items() if len(v) > 0]
        if len(last_scraped_url_indices) == 0:
            raise ValueError(
                "Cannot find index of the campaign {} in URLTABLE".format(
                    last_campaign_id
                )
            )
        last_scraped_url_index = last_scraped_url_indices[0]
        resume_index = last_scraped_url_index + 1
        print(f"Last scraped url was {last_scraped_url}")
        print(f"Last scraped index was {last_scraped_url_index}")
        print(f"Resuming scrape from index {resume_index}")
        self.URLTABLE = self.URLTABLE.loc[resume_index:, :]

    def start_from_specific_index(self, resume_index):
        print(f"Starting scrape from index {resume_index}")
        self.URLTABLE = self.URLTABLE.loc[resume_index:, :]

    def start_from_specific_campaign_id(self, campaign_id):
        resume_index = self.URLTABLE.index[
            self.URLTABLE["cleaned_campaign_id"] == campaign_id
        ].values[0]
        resume_url = self.URLTABLE.loc[resume_index, "cleaned_url"]
        print(f"URL to start scraping is {resume_url}")
        print(f"Starting scrape from index {resume_index}")
        self.URLTABLE = self.URLTABLE.loc[resume_index:, :]

    def create_gfm_urls_for_search_from_tablerow(self, w_url):
        urls_to_search = [w_url[self.URLTABLE_COLUMN]]
        if self.URLTABLE_COLUMN2 and not pd.isna(w_url[self.URLTABLE_COLUMN2]):
            # there's an extra url column we can include
            urls_to_search.append(w_url[self.URLTABLE_COLUMN2])
        if self.CAMPAIGNID_COLUMN and not pd.isna(w_url[self.CAMPAIGNID_COLUMN]):
            urls_to_search.append(
                "http://www.gofundme.com/" + w_url[self.CAMPAIGNID_COLUMN]
            )
            urls_to_search.append(
                "http://www.gofundme.com/f/" + w_url[self.CAMPAIGNID_COLUMN]
            )
        if self.CAMPAIGNID_COLUMN2 and not pd.isna(w_url[self.CAMPAIGNID_COLUMN2]):
            urls_to_search.append(
                "http://www.gofundme.com/" + w_url[self.CAMPAIGNID_COLUMN2]
            )
            urls_to_search.append(
                "http://www.gofundme.com/f/" + w_url[self.CAMPAIGNID_COLUMN2]
            )
        urls_to_search = [url_.replace("https", "http") for url_ in urls_to_search]
        urls_to_search = pd.unique(urls_to_search)
        return urls_to_search

    def construct_simple_gfm_url_for_wayback_query(self, url_, template="gofundme.com"):
        parsed = up.urlsplit(url_)
        base_url = template + parsed.path
        if parsed.query != "":
            base_url += "?" + parsed.query
        if parsed.fragment != "":
            base_url += "#" + parsed.fragment
        return base_url

    def deploy(self, resume=False, start_index=None, start_campaign=None):
        if resume:
            self.start_from_latest_ifile()
        elif start_index:
            self.start_from_specific_index(start_index)
        elif start_campaign:
            self.start_from_specific_campaign_id(start_campaign)

        i = 0
        for index, w_url in self.URLTABLE.iterrows():
            urls_to_search = self.create_gfm_urls_for_search_from_tablerow(w_url)
            print(f"Processing: {urls_to_search}")
            row_p, present_scrape_sucess = self.present_scrape_url(urls_to_search)
            if present_scrape_sucess or (not self.use_wayback):
                row = row_p
            else:
                # search with domain and prefix on wayback then scrape
                urls_to_search2 = [
                    self.construct_simple_gfm_url_for_wayback_query(_url)
                    for _url in urls_to_search
                ]
                urls_to_search2 = pd.unique(urls_to_search2)
                row_w = self.wayback_search_and_scrape(urls_to_search2)
                if (
                    (row_w["wayback_status"] == "wayback: success")
                    | (
                        row_w["wayback_status"]
                        == "wayback: scraped but did not meet success standard"
                    )
                    | (row_w["wayback_status"] == "wayback: inactive")
                ):
                    row = row_w
                    row["wayback_status"] = (
                        row_p["wayback_status"] + " ; " + row["wayback_status"]
                    )
                else:
                    row = row_p
                    row["wayback_status"] = (
                        row["wayback_status"] + " ; " + row_w["wayback_status"]
                    )

            if (index % self.NEW_FILE_THRESH) == 0:
                if i > 0:
                    outfile.close()  # close old file
                writer, outfile = self._create_new_outfile(
                    self._make_save_fullpath(index)
                )
                writer.writerow(row.keys())
            # if first loop but starting mid-dataframe
            elif i == 0:
                writer, outfile = self._load_outfile(self._get_save_fullpath(index))

            writer.writerow(row.values())
            i += 1
        outfile.close()


# Usage
# ScrapeManager().deploy()
# or
# ScrapeManager(use_wayback=False).deploy()
# ScrapeManager().deploy(resume=True)
# ScrapeManager().deploy(start_index=5)
# ScrapeManager().deploy(start_campaign='Supporting-local-artistic-ventures-')
