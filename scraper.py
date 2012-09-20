'''
Created on Aug 8, 2012

@author: timo
'''

from time import mktime, time as time_time, sleep
from datetime import time, datetime, date, timedelta
from scrapy.item import Item, Field
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.crawler import CrawlerProcess
import sched
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings

# Some settings.
OUTPUT_FILE = "/Users/timo/Documents/aptana-studio-3-workspace/scraper/output.csv"
SCHEDULE = [time(hour = 7), time(hour = 13), time(hour = 19), time(hour = 1)]

def make_infinite_daily_schedule(schedule):
    """
    Repeats the given schedule for each day, starting today and going till
    the end of time. The given schedule is a list of datetime.time objects.
    Returns a generator of datetime.datetime objects.
    """
    schedule = sorted(schedule)
    i = 0
    day = 0
    start_date = date.today()
    while True:
        yield datetime.combine(start_date + timedelta(days = day), schedule[i])
        if i + 1 == len(schedule):
            day = day + 1
            i = 0
        else:
            i = i + 1

def execute_daily(schedule, function):
    """
    Executes the given function each day, at the times specified in the
    schedule.
    """
    scheduler = sched.scheduler(time_time, sleep)
    for nextrun in make_infinite_daily_schedule(schedule):
        if nextrun >= datetime.now():
            print("Next run scheduled at " + nextrun.isoformat(" "))
            scheduler.enterabs(mktime(nextrun.timetuple()), 1, function, [])
            scheduler.run()

class GoldPriceItem(Item):
    """
    Item to hold the gold price.
    """
    price = Field()

class GoldPriceSpider(BaseSpider):
    """
    Spider for scraping web data.
    """
    name = "gold_price_spider"
    allowed_domains = ["stocks.migrosbank.ch"]
    start_urls = ["https://stocks.migrosbank.ch/www/market/rohstoffe"]
    
    def parse(self, response):
        selector = HtmlXPathSelector(response)
        gold_price_item = GoldPriceItem()
        html_elements = selector.select("//a[contains(text(), 'Gold')]/../following-sibling::*[2]/text()")
        gold_price_item["price"] = float(html_elements[0].extract().strip().replace("'", ""))
        return gold_price_item

def catch_item(sender, item, **kwargs):
    f = open(OUTPUT_FILE, "a")
    f.write(datetime.now().isoformat(" ") + "\t" + str(item["price"]) + "\n")
    f.close()

def scrape():
    dispatcher.connect(catch_item, signal = signals.item_passed)
    settings.overrides["LOG_ENABLED"] = False
    settings.overrides["DEPTH_LIMIT"] = 1
    settings.overrides["USER_AGENT"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1"
    crawler = CrawlerProcess(settings)
    crawler.install()
    crawler.configure()
    crawler.crawl(GoldPriceSpider())
    crawler.start()
    
if __name__ == "__main__":
    execute_daily(SCHEDULE, scrape)
