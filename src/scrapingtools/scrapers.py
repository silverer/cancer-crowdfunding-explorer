import unicodedata
import re

empty_row = {
    "url": "none",
    "last_donation_time": "none",
    "last_update_time": "none",
    "created_date": "none",
    "location": "none",
    "description": "none",
    "story": "none",
    "title": "none",
    "goal": "none",
    "tag": "none",
    "status": "none",
    "num_likes": "none",
    "num_shares": "none",
    "charity_details": "none",
    "error_message": "none",
}


def empty_url_row(url, msg="none"):
    erow = empty_row.copy()
    erow["url"] = str(url)
    erow["error_message"] = msg
    return erow


def is_inactive(soup):
    logic_1 = (
        len(
            soup.find_all(
                string=re.compile(
                    "Campaign is complete and no longer active", re.IGNORECASE
                )
            )
        )
        > 0
    )
    logic_2 = (
        len(
            soup.find_all(
                string=re.compile(
                    "fundraiser is no longer accepting donations", re.IGNORECASE
                )
            )
        )
        > 0
    )
    logic_3 = (
        len(
            soup.find_all(
                string=re.compile("currently disabled new donations", re.IGNORECASE)
            )
        )
        > 0
    )
    return logic_1 | logic_2 | logic_3


def not_found(soup):
    return (
        len(soup.find_all(string=re.compile("Campaign Not Found", re.IGNORECASE))) > 0
    )


def scrape_url_2015(soup, url):
    try:
        heart_container = soup.find_all("div", {"class": re.compile("fave-num")})
        num_likes = heart_container[0].text
    except:
        num_likes = "none"

    try:
        goal_container = soup.find_all("div", {"class": re.compile("raised")})
        goal = goal_container[0].text
    except:
        goal = "none"
    try:
        location_container = soup.find_all("a", {"class": re.compile("loc ")})
        location = location_container[0].text
    except:
        location = "none"

    try:
        story_container = soup.find_all("div", {"class": re.compile("pg_msg")})
        story = story_container[0].text
    except:
        story = "none"

    try:
        status_container = soup.find_all("div", {"class": re.compile("time")})
        status = status_container[0].text
    except:
        status = "none"

    try:
        created_date_container = soup.find_all("div", {"class": re.compile("cbdate")})
        created = created_date_container[0].text
    except:
        created = "none"

    try:
        share_container = soup.find_all("div", {"id": re.compile("top-share-bar")})
        num_shares = share_container[0].text
    except:
        num_shares = "none"

    try:
        description_container = soup.find_all("meta", {"name": "description"})
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:
        title = soup.title.string
    except:
        title = "none"

    try:
        tag_container = soup.find_all("a", {"class": "cat"})
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"
    try:
        amounts_container = soup.find_all("div", {"class": re.compile("damt")})
        time_container = soup.find_all("div", {"class": re.compile("dtime")})
        i = 0
        while "Update" in amounts_container[i].text:
            i += 1
        last_donation_amount = amounts_container[i].text
        last_donation_time = time_container[i].text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:  # done
        updates = soup.find_all("div", {"id": "allUpdates"})[0]
        last_update_time = updates.findChildren("div", {"class": "fr"})[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all("div", {"class": "charity-details"})
        charity_details = charity_container[0].text
    except:
        charity_details = "none"

    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode


def scrape_url_2017(soup, url):
    try:  # done
        heart_container = soup.find_all("div", {"class": re.compile(r"heart fave-num")})
        num_likes = heart_container[0].text
    except:
        num_likes = "none"

    try:  # done
        share_container = soup.find_all("strong", {"class": re.compile(r"share-count")})
        num_shares = share_container[0].text
    except:
        num_shares = "none"

    try:  # done
        goal_container = soup.find_all("h2", {"class": re.compile(r"goal")})
        goal = goal_container[0].text
    except:
        goal = "none"
    try:  # done
        location_container = soup.find_all("a", {"class": re.compile(r"location-name")})
        location = location_container[0].text
    except:
        location = "none"
    try:  # done
        story_container = soup.find_all(
            "div", {"id": re.compile(r"story.*description")}
        )
        story = story_container[0].text
    except:
        story = "none"

    try:  # done
        status_container = soup.find_all(
            "div", {"class": re.compile(r"campaign-status")}
        )
        status = status_container[0].text
    except:
        status = "none"

    try:  # done
        created_date_container = soup.find_all(
            "div", {"class": re.compile("created-date")}
        )
        created = created_date_container[0].text
    except:
        created = "none"

    try:  # done
        description_container = soup.find_all(
            "meta", {"name": re.compile(r"description")}
        )
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:  # done
        title = soup.title.string
    except:
        title = "none"

    try:  # done
        tag_container = soup.find_all("a", {"class": re.compile(r"category.*link")})
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"

    try:  # done
        amounts_container = soup.find_all(
            "div", {"class": re.compile(r"supporter-amount")}
        )
        time_container = soup.find_all("div", {"class": re.compile(r"supporter-time")})
        i = 0
        while "Update" in amounts_container[i].text:
            i += 1
        last_donation_amount = amounts_container[i].text
        last_donation_time = time_container[i].text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:
        time_container = soup.find_all("div", {"class": re.compile(r"supporter-time")})
        last_update_time = time_container[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all(
            "div", {"class": re.compile(r"charity-details")}
        )
        charity_details = charity_container[0].text
    except:
        charity_details = "none"
    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode


def scrape_url_2018(soup, url):
    try:
        heart_container = soup.find_all("div", {"class": re.compile(r"heart fave-num")})
        num_likes = heart_container[0].text
    except:
        num_likes = "none"
    try:
        share_container = soup.find_all("strong", {"class": re.compile(r"share-count")})
        num_shares = share_container[0].text
    except:
        num_shares = "none"

    try:
        status_container = soup.find_all(
            "div", {"class": re.compile(r"campaign-status")}
        )
        status = status_container[0].text
    except:
        status = "none"

    try:
        location_container = soup.find_all("a", {"class": re.compile(r"location")})
        location = location_container[0].text
    except:
        location = "none"

    try:
        created_date_container = soup.find_all(
            "div", {"class": re.compile(r"created-date")}
        )
        created = created_date_container[0].text
    except:
        created = "none"

    try:
        story_container = soup.find_all(
            "div", {"class": re.compile(r"story.*description")}
        )
        story = story_container[0].text
    except:
        story = "none"

    try:
        description_container = soup.find_all("meta", {"name": "description"})
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:
        goal_container = soup.find_all("h2", {"class": re.compile(r"goal")})
        goal = goal_container[0].text
    except:
        goal = "none"

    try:
        tag_container = soup.find_all("a", {"class": re.compile(r"category.*link")})
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"

    try:
        title = soup.title.string
    except:
        title = "none"

    try:
        amounts_container = soup.find_all(
            "div", {"class": re.compile(r"supporter-amount")}
        )
        time_container = soup.find_all("div", {"class": re.compile(r"supporter-time")})
        i = 0
        while "Update" in amounts_container[i].text:
            i += 1
        last_donation_amount = amounts_container[i].text
        last_donation_time = time_container[i].text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:
        time_container = soup.find_all("div", {"class": re.compile(r"supporter-time")})
        last_update_time = time_container[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all(
            "div", {"class": re.compile(r"charity-details")}
        )
        charity_details = charity_container[0].text
    except:
        charity_details = "none"

    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode


def scrape_url_2013(soup, url):

    try:  # retrieve number of friends instead
        heart_container = soup.find_all("p", {"class": re.compile(r"fby")})
        num_likes = heart_container[0].text
    except:
        num_likes = "none"

    try:  # retrieve number of comments instead
        share_container = soup.find_all("div", {"class": re.compile(r"social-share")})
        num_shares = share_container[0].text
    except:
        num_shares = "none"

    try:  # done
        status_container = soup.find_all("p", {"class": re.compile(r"rd_sub.*lts")})
        status = status_container[0].text
    except:
        status = "none"

    try:  # done
        location_container = soup.find_all("a", {"class": re.compile(r"place")})
        location = location_container[0].text
    except:
        location = "none"

    try:  # done
        created_date_container = soup.find_all("p", {"class": re.compile(r"abt_by")})
        created = created_date_container[0].text
    except:
        created = "none"

    try:  # done
        story_container = soup.find_all(
            "div", {"class": re.compile(r"abt.*(mid|post|text)")}
        )
        story = story_container[0].text
    except:
        story = "none"

    try:  # done
        description_container = soup.find_all("meta", {"name": "description"})
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:  # done
        goal_container = soup.find_all("div", {"class": "mtr1"})
        goal = goal_container[0].text
    except:
        goal = "none"

    try:  # done
        tag_container = soup.find_all("a", {"class": "category"})
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"

    try:  # done
        title = soup.title.string
    except:
        title = "none"

    try:  # done
        amounts_container = soup.find_all("p", {"class": "ml_16 damt mt_10 txt1"})
        time_container = soup.find_all("p", {"class": "ml_16 dtime it"})
        i = 0
        while "Update" in amounts_container[i].text:
            i += 1
        last_donation_amount = amounts_container[i].text
        last_donation_time = time_container[i].text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:
        update_container = soup.find_all("p", {"class": "ud_by"})
        last_update_time = update_container[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all("div", {"class": "charity-details"})
        charity_details = charity_container[0].text
    except:
        charity_details = "none"

    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode


def scrape_url_2012(soup, url):

    try:  # retrieve number of friends instead
        heart_container = soup.find_all("p", {"class": re.compile(r"fby")})
        num_likes = heart_container[0].text
    except:
        num_likes = "none"

    try:  # retrieve number of comments instead
        share_container = soup.find_all("div", {"class": "cmts_top geo it"})
        num_shares = share_container[0].text
    except:
        num_shares = "none"

    try:  # done
        status_container = soup.find_all("p", {"class": "rd lts"})
        status = status_container[0].text
    except:
        status = "none"

    try:  # done
        location_container = soup.find_all("a", {"class": "place"})
        location = location_container[0].text
    except:
        location = "none"

    try:  # done
        created_date_container = soup.find_all("p", {"class": "abt_by"})
        created = created_date_container[0].text
    except:
        created = "none"

    try:  # done
        story_container = soup.find_all("div", {"class": re.compile("abt_text")})
        story = story_container[0].text
    except:
        story = "none"

    try:  # done
        description_container = soup.find_all("meta", {"name": "description"})
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:  # done
        goal_container = soup.find_all("div", {"class": "mtr1"})
        goal = goal_container[0].text
    except:
        goal = "none"

    try:  # done
        tag_container = soup.find_all("a", {"class": "category"})
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"

    try:  # done
        title = soup.title.string
    except:
        title = "none"

    try:  # done
        amounts_container = soup.find_all("p", {"class": "ml_16 damt mt_10 txt1"})
        time_container = soup.find_all("p", {"class": "ml_16 dtime it"})
        i = 0
        while "Update" in amounts_container[i].text:
            i += 1
        last_donation_amount = amounts_container[i].text
        last_donation_time = time_container[i].text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:
        update_container = soup.find_all("p", {"class": "ud_by"})
        last_update_time = update_container[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all("div", {"class": "charity-details"})
        charity_details = charity_container[0].text
    except:
        charity_details = "none"

    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode


def scrape_url_2014(soup, url):

    try:  # retrieve number of friends instead
        heart_container = soup.find_all("p", {"class": "fb fby"})
        num_likes = heart_container[0].text
    except:
        num_likes = "none"

    try:  # retrieve number of comments instead
        share_container = soup.find_all("div", {"class": "cmts_top geo it"})
        num_shares = share_container[0].text
    except:
        num_shares = "none"

    try:  # done
        status_container = soup.find_all("p", {"class": "rd_sub lts"})
        status = status_container[0].text
    except:
        status = "none"

    try:  # done
        location_container = soup.find_all("a", {"class": "place"})
        location = location_container[0].text
    except:
        location = "none"

    try:  # done
        created_date_container = soup.find_all("p", {"class": "abt_by"})
        created = created_date_container[0].text
    except:
        created = "none"

    if created == "none":
        try:  # done
            created_date_container = soup.find_all(
                "span", {"class": re.compile(r"m-campaign-byline-created")}
            )
            created = created_date_container[0].text
        except:
            created = "none"

    try:  # done
        story_container = soup.find_all("div", {"class": re.compile("abt_mid")})
        story = story_container[0].text
    except:
        story = "none"

    try:  # done
        description_container = soup.find_all("meta", {"name": "description"})
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:  # done
        goal_container = soup.find_all("div", {"class": "mtr1"})
        goal = goal_container[0].text
    except:
        goal = "none"

    try:  # done
        tag_container = soup.find_all("a", {"class": "category"})
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"

    try:  # done
        title = soup.title.string
    except:
        title = "none"

    try:  # done
        amounts_container = soup.find_all("p", {"class": "ml_16 damt mt_10 txt1"})
        time_container = soup.find_all("p", {"class": "ml_16 dtime it"})
        i = 0
        while "Update" in amounts_container[i].text:
            i += 1
        last_donation_amount = amounts_container[i].text
        last_donation_time = time_container[i].text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:  # done
        update_container = soup.find_all("p", {"class": "ud_by"})
        last_update_time = update_container[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all("div", {"class": "charity-details"})
        charity_details = charity_container[0].text
    except:
        charity_details = "none"

    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode


def scrape_url_2019(soup, url):

    try:
        stats_container = soup.find_all("div", {"class": re.compile(r"social-stats")})[
            0
        ]
        num_likes = re.findall(r"\d+\s?followers", stats_container.text)[0]
        num_shares = re.findall(r"\d+\s?shares", stats_container.text)[0]
        status = re.findall(r"\d+\s?donors", stats_container.text)[0]
    except:
        try:
            stats_container = soup.find_all(
                "div", {"class": re.compile(r"m-social-stats")}
            )[0]
            num_likes = re.findall(r"\d+\s?followers", stats_container.text)[0]
            num_shares = re.findall(r"\d+\s?shares", stats_container.text)[0]
            status = re.findall(r"\d+\s?donors", stats_container.text)[0]
        except:
            num_likes = "none"
            num_shares = "none"
            status = "none"

    try:
        organizer_container = soup.find_all(
            "div", {"class": re.compile(r"campaign-members-main-organizer")}
        )[0]
        location = re.findall("Organizer(.+)", organizer_container.text, re.DOTALL)[0]
    except:
        location = "none"

    try:
        created_date_container = soup.find_all(
            "span", {"class": re.compile(r"campaign-byline-created")}
        )
        created = created_date_container[0].text
    except:
        created = "none"

    if created == "none":
        try:  # done
            created_date_container = soup.find_all(
                "span", {"class": re.compile(r"m-campaign-byline-created")}
            )
            created = created_date_container[0].text
        except:
            created = "none"

    if created == "none":
        try:  # done
            created_date_container = soup.find_all(
                "span", {"class": re.compile(r"created-date")}
            )
            created = created_date_container[0].text
        except:
            created = "none"

    try:
        story_container = soup.find_all("div", {"class": re.compile("campaign-story")})
        story = story_container[0].text
    except:
        story = "none"

    try:
        description_container = soup.find_all("meta", {"name": "description"})
        description = description_container[0].text
        if description == "":
            description = "none"
    except:
        description = "none"

    try:
        goal_container = soup.find_all(
            "div", {"class": re.compile(r"campaign-sidebar-progress-meter")}
        )
        goal = goal_container[0].text
    except:
        goal = "none"

    try:  # done
        tag_container = soup.find_all(
            "a", {"class": re.compile(r"campaign-byline-type")}
        )
        tag = tag_container[0].text
        tag = tag.replace("View All", "").strip()
    except:
        tag = "none"

    try:  # done
        title = soup.title.string
    except:
        title = "none"

    try:  # done
        donator_container = soup.find_all(
            "li", {"class": re.compile(r"donation-list-item")}
        )[0]
        donator_meta_container = donator_container.find_all(
            "ul", {"class": re.compile(r"donation")}
        )[0]
        amounts_container = donator_meta_container.find_all("li")[0]
        time_container = donator_meta_container.find_all("li")[1]
        last_donation_amount = amounts_container.text
        last_donation_time = time_container.text
    except:
        last_donation_amount = "none"
        last_donation_time = "none"

    try:  # done
        update_container = soup.find_all(
            "header", {"class": re.compile(r"update-info")}
        )
        last_update_time = update_container[0].text
    except:
        last_update_time = "none"

    try:
        charity_container = soup.find_all("div", {"class": "charity-details"})
        charity_details = charity_container[0].text
    except:
        charity_details = "none"

    row = {
        "url": url,
        "last_donation_time": last_donation_time,
        "last_update_time": last_update_time,
        "created_date": created,
        "location": location,
        "description": description,
        "story": story,
        "title": title,
        "goal": goal,
        "tag": tag,
        "status": status,
        "num_likes": num_likes,
        "num_shares": num_shares,
        "charity_details": charity_details,
        "error_message": "none",
    }

    try:
        row_encode = {
            k: unicodedata.normalize("NFKD", v.strip()) for k, v in row.items()
        }
    except Exception as e:
        print(e)
        row[
            "error_message"
        ] = "error normalizing encoding, return original values instead"
        row_encode = row

    return row_encode
