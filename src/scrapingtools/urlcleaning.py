# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 09:21:52 2019

@author: ers2244
"""


def omit_by_start(x):
    start = x.find("//")
    start += 2
    new = x[start:]
    if new[0:3] != "www":
        return True
    else:
        return False


def just_numbers(x):
    try:
        float(x)
    except:
        return False
    return True


def period_start(x):
    if x[0] == ".":
        return True
    else:
        return False


def format_hyphens(x):
    if x[0] == "-":
        return str(x)


def cut_percent_front(x):
    if "%" in x:
        loc = x.find("%")
        if loc == 0:
            return x
        return x[0:loc]
    else:
        return x


def remove_percent_within(x):
    if "%E2%80%8B" in x:
        x = x.replace("%E2%80%8B", "")
    if "%20" in x:
        x = x.replace("%20", "")
    if "%22" in x:
        x = x.replace("%22", "")
    if "%08" in x:
        x = x.replace("%08", "")
    return x


def first_char_weird(x):
    if x[0].isalpha():
        return False
    else:
        if x[0] == "-":
            return True
        elif x[0].isdigit():
            return False
        else:
            return True


def wayback_url_cleaning(wb_urls):
    delete_b = wb_urls.cleaned_url.apply(omit_by_start)
    # print(delete_b.value_counts())
    new = wb_urls[~delete_b]
    delete_b = new.cleaned_campaign_id.apply(just_numbers)
    # print(delete_b.value_counts())
    new = new[~delete_b]
    delete_b = new.cleaned_campaign_id.apply(period_start)
    new = new[~delete_b]
    new.cleaned_campaign_id = new.cleaned_campaign_id.apply(cut_percent_front)
    new.cleaned_campaign_id = new.cleaned_campaign_id.apply(remove_percent_within)
    delete_b = new.cleaned_campaign_id.apply(first_char_weird)
    new = new[~delete_b]
    new["cleaned_url"] = "http://www.gofundme.com/" + new.cleaned_campaign_id
    return new
