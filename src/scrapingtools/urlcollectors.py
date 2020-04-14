import datetime
import gzip
import shutil
import urllib.request
import xml.etree.ElementTree as et
from pathlib import Path
import warnings

import pandas as pd
import numpy as np

from .utils import log_message as print
from . import urlcleaning as uclean
from . import waybackinterface as wbi
from . import utils as utils


# from scrapingtools.utils import log_message as print
# import scrapingtools.urlcleaning as uclean
# import scrapingtools.waybackinterface as wbi
# import scrapingtools.utils as utils

GFM_SITEMAP_URL = "https://www.gofundme.com/sitemap.xml"
SITEMAP_PARENTDIR = Path.cwd().parent


class URLCollector(object):
    def __init__(
        self,
        gfm_sitemap_url=GFM_SITEMAP_URL,
        sitemap_parentdir=None,
        verbose=True,
        use_tqdm=True,
    ):
        self.gfm_sitemap_url = gfm_sitemap_url
        self.sitemap_store = sitemap_parentdir
        self.verbose = verbose
        self.use_tqdm = use_tqdm

    @property
    def use_tqdm(self):
        return self._use_tqdm

    @use_tqdm.setter
    def use_tqdm(self, mode):
        if mode:
            try:
                from tqdm import tqdm

                self.tqdm = tqdm
                self._use_tqdm = mode
            except ModuleNotFoundError:
                print("tqdm not found")
                self.use_tqdm = False
            except Exception as err:
                print(err)
                self.use_tqdm = False
        else:
            self._use_tqdm = False

    @property
    def sitemap_store(self):
        return self._sitemap_store

    @sitemap_store.setter
    def sitemap_store(self, dir):
        if dir is None:
            dir = SITEMAP_PARENTDIR
        dir = utils.verify_pathtype(dir)
        if not dir.exists():
            warnings.warn("{} not exists, will create directory".format(dir))
            dir.mkdir()
        self._sitemap_store = dir

    def log(self, *args):
        if self.verbose:
            print(*args)

    def _load_sitemap(self, sitemap_directory):
        """ returns today's sitemap (downloads it if it doesn't exist) """
        self.log("loading site data from " + self.gfm_sitemap_url)
        sitemap_path = sitemap_directory / "sitemap.xml"
        if not sitemap_path.exists():
            self.log(
                "time map is out of date; retreiving today's data and saving to "
                + str(sitemap_directory)
            )
            urllib.request.urlretrieve(self.gfm_sitemap_url, str(sitemap_path))
        else:
            self.log("site map is up-to-date; no need to download")
        return sitemap_directory, sitemap_path

    def _unpack_sitemap(self, sitemap_directory):
        dpath, fpath = self._load_sitemap(sitemap_directory)
        gzs = []
        tree = et.parse(fpath)
        root = tree.getroot()
        savepath = dpath / "packets"
        if not savepath.exists():
            self.log("loading packets...")
            savepath.mkdir()
            if self.use_tqdm:
                root = self.tqdm(root, total=len(root))
            for sitemap in root:
                sitemap_iter = sitemap.findall(
                    "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                )
                for gz in sitemap_iter:
                    filename = gz.text.split("/")[-1]
                    if filename == "sitemap_marketing.xml.gz":
                        continue
                    if self.use_tqdm:
                        root.set_description("retrieved " + filename)
                    else:
                        self.log("---retrieved " + filename)
                    filepath = savepath / filename
                    urllib.request.urlretrieve(gz.text, str(filepath))
                    gzs.append(filepath)
        else:
            self.log("site map packets already exist; no need to unpack.")
            gzs = [p for p in savepath.glob("*.gz") if p.is_file()]
        return gzs

    def _unzip_all_gzs(self, gzs):
        unzipped = [(gz.parent / gz.stem) if gz.suffix == ".gz" else gz for gz in gzs]
        iterr = zip(gzs, unzipped)
        if self.use_tqdm:
            iterr = self.tqdm(iterr, total=len(unzipped))
        for gz, uz in iterr:
            if self.use_tqdm:
                iterr.set_description("unzipping " + (gz.stem + gz.suffix))
            if gz.suffix != ".gz":
                continue
            with gzip.open(str(gz), "r") as f_in, uz.open(mode="wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            # os.remove(gz)
        return unzipped

    def _read_urls_from_xml_file(self, filepath):
        """This parses the xml files to get the available urls.
        Checks for the subset that have the work 'cancer' in their url.
        This is not a great filter, but works for the time being. """
        if not self.use_tqdm:
            self.log("parsing " + str(filepath.stem))
        root = et.parse(filepath).getroot()
        urls = [
            loc.text
            for _url in root
            for loc in _url.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        ]
        df = pd.Series(urls).to_frame(name="url")
        # self.log('testing: dataframe length=' + str(len(df)))
        return df

    def create_url_table(self):
        """This goes through the site map and tabulates all the available urls.
        The 'cancer' condition is impleneted in read_urls_from_xml_file at the moment.
        Should probably move it."""
        today_sitemap_folder = "sitemap_" + datetime.datetime.now().strftime("%Y%m%d")
        sitemap_directory = self.sitemap_store / today_sitemap_folder
        tablepath = sitemap_directory / "gfm_urls.csv"
        if not tablepath.parent.exists():
            tablepath.parent.mkdir(parents=True)
        uz_filepaths = self._unzip_all_gzs(self._unpack_sitemap(sitemap_directory))
        if self.use_tqdm:
            uz_filepaths = self.tqdm(uz_filepaths, desc="parsing sitemap xml")
        urls_df_l = []
        for fp in uz_filepaths:
            if self.use_tqdm:
                uz_filepaths.set_description("parsing " + str(fp.stem))
            urls_df_l.append(self._read_urls_from_xml_file(fp))
        urls_df = pd.concat(urls_df_l, ignore_index=True, sort=True)
        self.log("{} urls loaded successfully".format(urls_df.shape[0]))
        urls_df.to_csv(tablepath, encoding="utf-8", index=False)
        return tablepath, urls_df


class WaybackURLCollector(object):
    def __init__(
        self, start_year=2012, end_year=2019, sitemap_parentdir=None, use_tqdm=True
    ):
        self.use_tqdm = use_tqdm
        (
            self.savepath,
            self.query_outputpath,
            self.url_outputpath,
        ) = self.create_savepaths(sitemap_parentdir)
        self.URLTABLE_BATCH_SIZE = 100
        self.MAX_WAYBACK_SEARCH_PAGE = wbi.get_max_search_page()
        self.start_year, self.end_year = start_year, end_year

    @property
    def use_tqdm(self):
        return self._use_tqdm

    @use_tqdm.setter
    def use_tqdm(self, mode):
        if mode:
            try:
                from tqdm import tqdm

                self.tqdm = tqdm
                self._use_tqdm = mode
            except ModuleNotFoundError:
                print("tqdm not found")
                self.use_tqdm = False
            except Exception as err:
                print(err)
                self.use_tqdm = False
        else:
            self._use_tqdm = False

    def create_savepaths(self, dir):
        if dir is None:
            dir = SITEMAP_PARENTDIR
        utils.verify_pathtype(dir)
        if not dir.exists():
            warnings.warn("{} not exists, will create directory".format(dir))
            dir.mkdir()
        today_wayback_folder = "wayback_" + datetime.datetime.now().strftime("%Y%m%d")
        savepath = dir / today_wayback_folder
        if not savepath.exists():
            savepath.mkdir()
        output_fdir = savepath / "query_output"
        if not output_fdir.exists():
            output_fdir.mkdir()
        url_fdir = savepath / "url_batch"
        if not url_fdir.exists():
            url_fdir.mkdir()
        return savepath, output_fdir, url_fdir

    def domain_search_query(self, i):
        wayback_header = r"http://web.archive.org/cdx/search/cdx?"
        domain_str = r"url=gofundme.com&matchType=domain"
        years_str = r"&from={}&to={}".format(self.start_year, self.end_year)
        page_num_str = r"&page={}".format(i)
        return wayback_header + domain_str + years_str + page_num_str

    def process_query(self, url_i):
        return pd.read_csv(
            url_i,
            sep=" ",
            header=None,
            names=wbi.SEARCH_RESULT_HEADER,
            encoding="utf-8",
        )

    def create_url_table(self, return_condensed=True):
        batch_size, max_page = self.URLTABLE_BATCH_SIZE, self.MAX_WAYBACK_SEARCH_PAGE
        out_fpaths, url_fpaths = [], []
        output_fdir = self.query_outputpath
        for ibatch, batch in enumerate(
            np.split(
                np.arange(max_page, dtype=np.uint32),
                np.arange(batch_size, max_page, batch_size, dtype=np.uint32),
            )
        ):
            df_l = []
            for ipage in batch:
                print(f"Querying page {ipage}")
                query = self.domain_search_query(ipage)
                df_l.append(wbi.pull_request(query, self.process_query))
            df_a = pd.concat(df_l, ignore_index=True, sort=True)
            print(f"Saving batch {ibatch}")
            output_fpath = output_fdir / f"wayback_query_output_batch{ibatch}.csv"
            df_a.to_csv(output_fpath, encoding="utf-8", index=False)
            out_fpaths.append(output_fpath)
            # clean output to get url list
            url_fpath_l = self.extract_urls_from_wayback_output([output_fpath])
            url_fpaths += url_fpath_l
        # read all urls to get massive dataframe
        if return_condensed:
            return self.condense_url_batches(url_fpaths)
        else:
            return url_fpaths, None

    def condense_url_batches(self, url_fpaths):
        url_table = pd.concat(
            [pd.read_csv(ufp, encoding="utf-8") for ufp in url_fpaths],
            ignore_index=True,
            sort=False,
        )
        fout = self.savepath / "wayback_urls_all_batches.csv"
        print("Saving {}".format(str(fout)))
        url_table.to_csv(fout, index=False)
        return fout, url_table

    def extract_urls_from_wayback_output(self, out_fpaths):
        if self.use_tqdm and (len(out_fpaths) > 3):
            ofpath_iter = self.tqdm(
                out_fpaths, total=len(out_fpaths), desc="Extracting urls"
            )
        else:
            ofpath_iter = out_fpaths
        url_fpaths = []
        url_fdir = self.url_outputpath
        cleaned_table_columns = [
            "parsed_url",
            "campaign_id",
            "cleaned_url",
            "cleaned_campaign_id",
            "original_url",
        ]
        for ofpath in ofpath_iter:
            urls = pd.read_csv(ofpath, dtype="str")
            if urls.empty:
                cleaned_campaign_urls = pd.DataFrame(columns=cleaned_table_columns)
            else:
                print(f"Cleaning {ofpath.stem}")
                unique_campaign_urls = wbi.drop_duplicate_wayback_url(
                    urls, self.use_tqdm
                )
                if unique_campaign_urls.empty:
                    cleaned_campaign_urls = pd.DataFrame(columns=cleaned_table_columns)
                else:
                    cleaned_campaign_urls = uclean.wayback_url_cleaning(
                        unique_campaign_urls
                    )
            # save result
            url_path = url_fdir / (
                ofpath.stem.replace("query_output", "urls") + ofpath.suffix
            )
            cleaned_campaign_urls.to_csv(url_path, encoding="utf-8", index=False)
            url_fpaths.append(url_path)
        return url_fpaths

    def compare_url_tables(self, gfm_urls, wb_urls):
        master_table, wb_only_urls = wbi.compare_url_tables(gfm_urls, wb_urls)
        wb_only_outpath = self.savepath / "wayback_urls_not_in_sitemap.csv"
        print("Saving " + str(wb_only_outpath))
        wb_only_urls.to_csv(wb_only_outpath, encoding="utf-8", index=False)
        master_path = self.savepath / "master_urls_table.csv"
        print("Saving " + str(master_path))
        master_table.to_csv(master_path, encoding="utf-8", index=False)
        return master_path, master_table


# Usage
# tablepath,urls_df=URLCollector().create_url_table()
# wbcollector = WaybackURLCollector()
# wbtablepath,wburls_df=wbcollector.create_url_table()
# masterpath,master_df = wbcollector.compare_url_tables(urls_df,wburls_df)
