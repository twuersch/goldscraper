'''
Created on Aug 8, 2012

@author: timo
'''

from time import mktime, time as time_time, sleep
from datetime import datetime, date, timedelta
import sched
import random
import requests
from StringIO import StringIO
from lxml import etree
from optparse import OptionParser

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

def execute_daily(schedule, function, max_rnd_offset = 0, **kwargs):
    """
    Executes the given function each day, at the times specified in the
    schedule, including a randomized offset.
    """
    scheduler = sched.scheduler(time_time, sleep)
    for nextrun in (t + timedelta(seconds = max_rnd_offset * random.uniform(-1.0, 1.0)) for t in make_infinite_daily_schedule(schedule)):
        if nextrun >= datetime.now():
            print("Next run scheduled at " + nextrun.isoformat(" "))
            scheduler.enterabs(mktime(nextrun.timetuple()), 1, function, [kwargs])
            scheduler.run()
    
def scrape_gold_price(kwargs):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1"}
    timestamp = datetime.now()
    try:
        r = requests.get("http://stocks.migrosbank.ch/www/market/rohstoffe", headers=headers)
        if r.status_code != 200:
            raise Exception("Got HTTP status code " + str(r.status_code))
        parser = etree.HTMLParser()
        tree = etree.parse(StringIO(r.text), parser)
        elements = tree.xpath("//a[contains(text(), 'Gold')]/../following-sibling::*[2]")
        gold_price = float(elements[0].text.strip().replace("'", ""))
        print(timestamp.isoformat(" ") + "\t" + str(gold_price))
        
        if "filename" in kwargs and kwargs["filename"] is not None:
            f = open(kwargs["filename"], "a")
            f.write(timestamp.isoformat(" ") + "\t" + str(gold_price) + "\n")
            f.close()
    except Exception as exception:
        print("Exception at " + timestamp.isoformat(" ") + ": " + str(exception))

if __name__ == "__main__":
    usage = "Usage: scraper [options]"
    option_parser = OptionParser(usage = usage)
    option_parser.add_option("-o", "--output", dest="filename", help="Appends values to FILE. If omitted, writes to stdout only.", metavar="FILE")
    option_parser.add_option("-s", "--schedule", dest="schedule", help="Specify the daily schedule as a list of Python time objects, e.g. \"[time(hour = 7), time(hour = 13)]\". If omitted, execute only once.")
    option_parser.add_option("-r", "--max-rnd-offset", dest="max_rnd_offset", help="Randomizes the schedule by a maximum of SECONDS. Defaults to 0.", metavar="SECONDS")
    options, args = option_parser.parse_args()

    max_rnd_offset = int(options.max_rnd_offset) if options.max_rnd_offset else 0
    filename = options.filename
    if options.schedule:
        # This import is requred for the eval() on the following line
        from datetime import time
        schedule = eval(options.schedule)
        execute_daily(schedule, scrape_gold_price, max_rnd_offset=max_rnd_offset, filename=filename)
    else:
        scrape_gold_price(filename)
