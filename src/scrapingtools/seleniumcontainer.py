import time
from .utils import log_message as print
import argparse
from sys import platform
from selenium import webdriver
import random
RETRY_MAX = 5  # times
TIMEOUT = 150  # seconds
SLEEP = 30  # seconds

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class RendererContainer(object):
    def __init__(self):
        self.renderer = None
        self.get_renderer()

    def get_renderer(self):
        if self.renderer is None:
            self.renderer = webdriver.Chrome()
        return self.renderer

    def render(self, url):
        retry = True
        retry_count = 0
        print(f"Requesting {url}")
        campaign_page  = AttrDict()
        campaign_page.url = url
        while retry:
            try:
                time.sleep(random.randint(0,10))
                self.renderer.get(url)
                campaign_page.text = self.renderer.page_source
                if campaign_page.text is None: raise Exception()
                retry = False
            except Exception as e:
                print("[rendercontainer] " + str(e))
                if retry_count < RETRY_MAX:
                    retry = True
                    print(f"[rendercontainer] sleep {SLEEP} secs before retrying request")
                    time.sleep(SLEEP)
                else:
                    print(f"[rendercontainer] failed to request {retry_count} times")
                    retry = False
                    campaign_page = None
            retry_count += 1
        return campaign_page

    def default_callback(self):
        def cb(url, html):
            ad = AttrDict()
            ad["url"] = url
            ad["text"] = html
            return ad

        return cb


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("url", type=str)
    args = parser.parse_args()

    wr = RendererContainer()
    page = wr.render(args.url)
    print(page)
