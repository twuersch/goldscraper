'''
Created on Aug 8, 2012

@author: timo
'''

from time import mktime, time as time_time, sleep
from datetime import time, datetime, date, timedelta
import sched
import random
import requests
from StringIO import StringIO
from lxml import etree

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

def execute_daily_randomized(schedule, function):
    """
    Executes the given function each day, at the times specified in the
    schedule, including a randomized offset.
    """
    scheduler = sched.scheduler(time_time, sleep)
    for nextrun in (t + timedelta(seconds = 0 * random.uniform(-1.0, 1.0)) for t in make_infinite_daily_schedule(schedule)):
        if nextrun >= datetime.now():
            print("Next run scheduled at " + nextrun.isoformat(" "))
            scheduler.enterabs(mktime(nextrun.timetuple()), 1, function, [])
            scheduler.run()
    
def get_gold_price():
    r = requests.get("https://stocks.migrosbank.ch/www/market/rohstoffe", headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1"})
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(r.text), parser)
    elements = tree.xpath("//a[contains(text(), 'Gold')]/../following-sibling::*[2]")
    gold_price = float(elements[0].text.strip().replace("'", ""))
    return gold_price

def write_gold_price_to_file(price):
    f = open(OUTPUT_FILE, "a")
    f.write(datetime.now().isoformat(" ") + "\t" + str(price) + "\n")
    f.close()

def scrape_gold_price():
    write_gold_price_to_file(get_gold_price())

if __name__ == "__main__":
    execute_daily_randomized(SCHEDULE, scrape_gold_price)
