import requests
import pandas as pd
import urllib.parse as up
import time
import re
from .utils import log_message as print
from tqdm import tqdm
from io import StringIO

RETRY_MAX = 10  # times
TIMEOUT = 150  # seconds
SLEEP = 60  # seconds
SEARCH_RESULT_HEADER = [
    "urlkey",
    "timestamp",
    "original",
    "mimetype",
    "statuscode",
    "digest",
    "length",
]

IMPORTANCE_MATRIX = pd.DataFrame.from_dict(
    {   "url": 0,
        "last_donation_time":1,
        "last_update_time": 1,
        "created_date": 4,
        "location_city": 4,
        "location_country": 4,
        "location_postalcode": 0,
        "location_stateprefix": 1,
        "description": 1,
        "poster": 1,
        "story": 4,
        "title": 2,
        "goal": 3,
        "raised_amnt": 3,
        "goal_amnt": 3,
        "currency": 3,
        "tag": 2,
        "num_donors": 3,
        "num_likes":2,
        "num_shares": 2,
        "charity_details": 1,
        "error_message": 0,
    },
    orient="index",
    columns=["importance_score"],
)
IMPORTANCE_MATRIX.index.name = "field"
IMPORTANCE_MATRIX = IMPORTANCE_MATRIX["importance_score"]


def get_max_search_page():
    npage_query = r"http://web.archive.org/cdx/search/cdx?"
    npage_query += r"url=gofundme.com&matchType=domain"
    npage_query += r"&showNumPages=true"
    try:
        npage = int(requests.get(npage_query).text.strip())
    except Exception as ee:
        print(ee)
        npage = 1775  # good estimate retrieved from browser
    return npage


def pull_request(url_to_search, process_func, **kwargs):
    retry = True
    for retry_count in range(RETRY_MAX):
        if retry_count > 0:
            print(f"request try {retry_count+1}")
        try:
            results = process_func(url_to_search, **kwargs)
            if results.empty:
                results = pd.DataFrame(columns=SEARCH_RESULT_HEADER)
            retry = False
            if not retry:
                break
        except Exception as ee:
            print(f"returned error: {str(ee)}")
            if retry_count < (RETRY_MAX - 1):
                print(f"sleep {SLEEP} secs before retrying request")
                time.sleep(SLEEP)
    if retry:
        print(f"failed to search {retry_count+1} times")
        results = pd.DataFrame(columns=SEARCH_RESULT_HEADER)
    return results


def _search_wayback(url_to_search, timeout=TIMEOUT):
    # api_query = f"http://archive.org/wayback/available?url={url_to_search}"
    # search_way_one = f'http://timetravel.mementoweb.org/api/json/2014/{url_to_search}'
    # search_way_2 = f'http://web.archive.org/web/timemap/link/{url_to_search}'
    search_way_3 = (
        f"http://web.archive.org/cdx/search/cdx?url={url_to_search}"
        + "&matchType=prefix&output=json"
    )
    print(f"Search for archives w query: {search_way_3}")
    search_page = requests.get(search_way_3, timeout=timeout)
    search_results = pd.read_json(StringIO(search_page.text), encoding="utf-8")
    if not search_results.empty:
        search_results = search_results.rename(columns=search_results.iloc[0, :]).drop(
            index=0
        )
        search_results = search_results.sort_values(by="timestamp").reset_index(
            drop=True
        )
    return search_results


def search_wayback(url_to_search, timeout=TIMEOUT):
    return pull_request(url_to_search, _search_wayback, timeout=timeout)


def clean_wayback_search_results(queryoutput):
    parsed_urls = queryoutput.original.apply(lambda x: up.urlsplit(x))
    parsed_urls = pd.DataFrame.from_records(
        parsed_urls,
        columns=["_scheme", "_domain", "_path", "_query", "_fragment"],
        index=parsed_urls.index,
    ).join(queryoutput, how="left")
    parsed_urls["unquote_path"] = parsed_urls._path.apply(lambda x: up.unquote(x))
    parsed_urls["unquote_query"] = parsed_urls._query.apply(lambda x: up.unquote(x))
    select_urls = parsed_urls.groupby(by=["unquote_path"], as_index=False).apply(
        lambda df: df.loc[df.unquote_query == df.unquote_query.min(), :]
    )
    select_urls = select_urls.sort_values("timestamp", ascending=False).reset_index(
        drop=True
    )
    select_urls["original"] = (
        select_urls["original"].astype(str).apply(remove_port_from_url)
    )
    return select_urls


def remove_port_from_url(url_str):
    return url_str.replace(":80", "")


def filter_nonworking_search_results(search_results):
    search_results = search_results[~(search_results.statuscode.isin(["301", "404"]))]
    return search_results


def get_campaign_page(url_to_get, check_status_code=True):
    retry = True
    print(f"Requesting {url_to_get}")
    for retry_count in range(RETRY_MAX):
        if retry_count > 0:
            print(f"request try {retry_count+1}")
        try:
            campaign_page = requests.get(url_to_get)
            if check_status_code:
                retry = campaign_page.status_code != 200
                # 200 is http response code for success
                if retry and (retry_count == (RETRY_MAX - 1)):
                    raise Exception("http status code is not 200")
            else:
                retry = False
            if not retry:
                break
        except Exception as ee:
            print(f"returned error: {ee}")
            if retry_count < (RETRY_MAX - 1):
                print(f"sleep {SLEEP} secs before retrying request")
                time.sleep(SLEEP)
    if retry:
        print(f"failed to request {retry_count+1} times")
        campaign_page = None
    return campaign_page


def scrape_quality(row):
    not_none = pd.Series(row) != "none"
    return (not_none * IMPORTANCE_MATRIX).sum()


def timestamp_from_wayback_url(url_str):
    return re.findall(r"http://web.archive.org/web/(\d*)/", url_str)[0]


def remove_encode_chars(url_str):
    encode_chars = ["%40", "%20", "%21"]
    x = up.urlsplit(url_str)
    sr_keys = ["scheme", "netloc", "path", "query", "fragment"]
    sr_dict = {k: v for k, v in zip(sr_keys, x)}
    for echar in encode_chars:
        sr_dict["path"] = sr_dict["path"].replace(echar, "")
    sr_dict["query"] = up.quote(sr_dict["query"])
    return up.urlunsplit(sr_dict.values())


def unsplit_url(x):
    sr_keys = ["scheme", "netloc", "path", "query", "fragment"]
    sr_dict = {k: v for k, v in zip(sr_keys, x)}
    sr_dict["path"] = up.quote(sr_dict["path"])
    sr_dict["query"] = up.quote(sr_dict["query"])
    return up.urlunsplit(sr_dict.values())


def choose_min_query(df):
    return df.loc[df.parsed_query == df.parsed_query.min(), :]


def get_extra_path(x):
    x_l = re.split(r"[/&]+", x)
    if len(x_l) <= 1:
        return ""  # happens when the url is just http://www.gofundme.com/
    return "/".join(x_l[2:])


def choose_first_path(x):
    x_l = re.split(r"[/&]+", x)
    if len(x_l) <= 1:
        return ""  # happens when the url is just http://www.gofundme.com/
    return x_l[1]


def extract_campaign_id_from_gfm_url(x):
    return up.urlsplit(x).path.split("/")[-1]


def remove_special_char_in_beginning(x):
    if x == "":
        return x

    def _check_normal(x):
        return re.match(r"[a-z0-9\-]", x, re.I)

    # x is encoded
    x_u = up.unquote(x)
    encoded = x[0] != x_u[0]
    # if True, encoded, False: not encoded
    if encoded:
        if not _check_normal(x_u):
            special_char = x_u[0]
            qspecial_char = up.quote(special_char)
            x = x.replace(qspecial_char, "")
    else:
        if not _check_normal(x):
            x = x[1:]
    return x


def remove_ending_period(x):
    if x == "":
        return x
    elif x[-1] == ".":
        x = x[:-1]
    return x


def find_hidden_query_in_path(p):
    # match stuf in pattern &pc=example&cache=1234 and remove it from string
    for found in re.findall(r"&.+=.*", p, re.I):
        p = p.replace(found, "")
    return p


def drop_duplicate_wayback_url(queryoutput, use_tqdm=True):
    parsed_urls = queryoutput.original.apply(lambda x: up.urlsplit(x))
    parsed_urls = pd.DataFrame.from_records(
        parsed_urls,
        columns=["_scheme", "_domain", "_path", "_query", "_fragment"],
        index=parsed_urls.index,
    )

    parsed_urls["_path_parts"] = parsed_urls._path.str.split("/").apply(lambda x: x[1:])
    parsed_urls["_path_len"] = parsed_urls._path_parts.apply(lambda x: len(x))
    campaign_ids_ = parsed_urls.loc[parsed_urls._path_len > 0, "_path_parts"].apply(
        lambda x: x[0]
    )
    parsed_urls["campaign_ids"] = ""
    parsed_urls.campaign_ids.update(campaign_ids_)
    campaign_ids = parsed_urls.campaign_ids
    urls = pd.DataFrame(
        {
            "parsed_url": "http://www.gofundme.com/" + campaign_ids,
            "campaign_id": campaign_ids,
        }
    )

    cleaned_campaign_ids = campaign_ids.apply(remove_special_char_in_beginning)
    cleaned_campaign_ids = cleaned_campaign_ids.apply(find_hidden_query_in_path)
    cleaned_campaign_ids = cleaned_campaign_ids.apply(remove_ending_period)
    cleaned_campaign_ids.name = "cleaned_campaign_id"
    cleaned_urls = pd.DataFrame(
        {
            "cleaned_url": "http://www.gofundme.com/" + cleaned_campaign_ids,
            "cleaned_campaign_id": cleaned_campaign_ids,
        }
    )

    bad_domains = [  # these domains dont contain campaign ids
        "images.gofundme.com",
        "support.gofundme.com",
        "developer.gofundme.com",
        "api.gofundme.com",
        "email.gofundme.com",
    ]
    parsed_urls["_domain"] = (
        parsed_urls._domain.apply(remove_port_from_url).str.lower().str.strip()
    )

    all_urls = (
        urls.join(cleaned_urls, how="left")
        .join(parsed_urls._domain, how="left")
        .join(queryoutput.original, how="left")
    )
    all_urls = all_urls[~all_urls._domain.isin(bad_domains)]
    all_urls = all_urls.drop(columns=["_domain"])

    all_urls = (
        all_urls[cleaned_campaign_ids != ""]
        .drop_duplicates(subset=["cleaned_campaign_id"])
        .reset_index(drop=True)
    )
    return all_urls


def compare_url_tables(gfm_urls, wb_urls):
    gfm_urls = gfm_urls.copy()
    gfm_urls.columns = ["original_url"]
    gfm_urls["campaign_id"] = gfm_urls.original_url.apply(
        extract_campaign_id_from_gfm_url
    )
    wb_only_urls = wb_urls[~wb_urls["cleaned_campaign_id"].isin(gfm_urls.campaign_id)]

    gfm_urls["cleaned_url"] = gfm_urls.original_url
    gfm_urls["cleaned_campaign_id"] = gfm_urls.campaign_id
    master_table = pd.concat([gfm_urls, wb_only_urls], ignore_index=True, sort=False)
    return master_table, wb_only_urls
