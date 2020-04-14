import socket
import urllib.request
import urllib.error
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import datetime


class TqdmUpTo(tqdm):
    """
    Simple tqdm wrapper on urllib.request for downloading large files
    Based on https://github.com/tqdm/tqdm/blob/master/examples/tqdm_wget.py
    """

    """
    tqdm instance wrapped for urlretrieve reporthook
    Provides `update_to(n)` which uses `tqdm.update(delta_n)`.
    Inspired by [twine#242](https://github.com/pypa/twine/pull/242),
    [here](https://github.com/pypa/twine/commit/42e55e06).
    """

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


def download_file(source, desc=None, retries=5, savedir="state_census_population.csv"):
    """
    Download a single file from the give source url to a temporary location,
        displaying a progress bar while downloading, then returns the location
        of the downloaded file
    """
    socket.setdefaulttimeout(30)
    attempt = 0
    while attempt < retries:
        try:
            with TqdmUpTo(
                unit="B", unit_scale=True, unit_divisor=1024, miniters=1, desc=desc
            ) as t:
                file, _ = urllib.request.urlretrieve(
                    source, filename=savedir, reporthook=t.update_to
                )
                break
        except (socket.timeout, urllib.error.URLError):
            attempt += 1
            t.write(f"Starting attempt {attempt + 1}")

    if file is None:
        raise RuntimeError(f"Couldn't download {source} in {retries} attempts")

    return file


CENSUS_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2018/national/totals/nst-est2018-alldata.csv"
CENSUS_filedir = Path.cwd() / "shapefiles" / "state_census_population.csv"

us_state_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Northern Mariana Islands": "MP",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Palau": "PW",
    "Pennsylvania": "PA",
    "Puerto Rico": "PR",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virgin Islands": "VI",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

state_fips = {
    "WA": "53",
    "DE": "10",
    "DC": "11",
    "WI": "55",
    "WV": "54",
    "HI": "15",
    "FL": "12",
    "WY": "56",
    "PR": "72",
    "NJ": "34",
    "NM": "35",
    "TX": "48",
    "LA": "22",
    "NC": "37",
    "ND": "38",
    "NE": "31",
    "TN": "47",
    "NY": "36",
    "PA": "42",
    "AK": "02",
    "NV": "32",
    "NH": "33",
    "VA": "51",
    "CO": "08",
    "CA": "06",
    "AL": "01",
    "AR": "05",
    "VT": "50",
    "IL": "17",
    "GA": "13",
    "IN": "18",
    "IA": "19",
    "MA": "25",
    "AZ": "04",
    "ID": "16",
    "CT": "09",
    "ME": "23",
    "MD": "24",
    "OK": "40",
    "OH": "39",
    "UT": "49",
    "MO": "29",
    "MN": "27",
    "MI": "26",
    "RI": "44",
    "KS": "20",
    "MT": "30",
    "MS": "28",
    "SC": "45",
    "KY": "21",
    "OR": "41",
    "SD": "46",
}


def format_county_fip(x):
    if pd.isnull(x):
        return "nan"
    elif type(x) == str:
        if len(x) == 3:
            return x
        if len(x) == 1:
            return "00" + x
        elif len(x) == 2:
            return "0" + x
    else:
        if x < 10:
            return "00" + str(int(x))
        elif x < 100:
            return "0" + str(int(x))
        else:
            return str(int(x))


def format_state_fip(x):
    if pd.isnull(x):
        return "nan"
    elif type(x) == str:
        if len(x) == 1:
            return "0" + x
        else:
            return x
    else:
        x = str(int(x))
        if len(x) < 2:
            return "0" + x
        else:
            return x


def format_state_county_fip(x):
    if pd.isnull(x):
        return "nan"
    if type(x) == str:
        return "nan"
    else:
        x = str(int(x))
        if len(x) < 5:
            x = "0" + x
        return x


def rgb_to_hex(rgb_arr):
    r, g, b = rgb_arr
    return "#%02x%02x%02x" % (r, g, b)


def construct_colorscale_from_cmap(cmap, nlevels):
    rgb_colorscale = (cmap(np.linspace(0, 1, nlevels)) * 256).astype(int)[:, :3]
    hex_colorscale = list(np.apply_along_axis(rgb_to_hex, arr=rgb_colorscale, axis=1))
    return hex_colorscale


def log_message(*args, p_fn=print):
    text = " ".join([str(x) for x in args])
    p_fn(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: {text}')
    return


state_list = [
    "AK",
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
]
