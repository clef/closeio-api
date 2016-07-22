#!/usr/bin/env python
import argparse
import datetime
import json
from datetime import timedelta
import dateutil.parser
import copy
from closeio_api import Client as CloseIO_API
from tabulate import tabulate
from collections import (
    defaultdict,
    Counter,
    OrderedDict
)
from ordered_set import OrderedSet

parser = argparse.ArgumentParser(description='Detect duplicates & merge leads (see source code for details)')
parser.add_argument('--api-key', '-k', required=True, help='API Key')
parser.add_argument('--segment', required=False, help='A property to segment leads on')
parser.add_argument('--segment-default-value', required=False, help='A default value for the property to segment leads on')
parser.add_argument('--weeks', type=int, required=False, help='The number of weeks to look at historically')
parser.add_argument('--include-not-native', required=False, default=False, help='Whether we should include basic and business customers')
parser.add_argument('--format', required=False, choices=['pretty', 'json', 'csv'], default='pretty', help='The format you want the data to be exported in')
parser.add_argument('--funnel-format', required=False, choices=['raw', 'percentage'], default='raw', help='Whether you want the raw funnel or percentages')
args = parser.parse_args()

"""
Segment leads by created at and print out funnel analysis
"""

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, OrderedSet):
            return list(obj)
        else:
            return super(DateTimeEncoder, self).default(obj)

class OrderedCounter(Counter, OrderedDict):
    pass

desired_status = 'open' # capitalization doesn't matter

api = CloseIO_API(args.api_key)
status_order = [
    "Prospect",
    "Approaching",
    "Working",
    "Qualified",
    "Customer",
]
status_exclude = [
    "Bad Fit",
    "Dead"
]

def start_of_week(dt):
    as_date = dt.date()
    return as_date - timedelta(days=as_date.weekday())

def statuses_for_leads(leads, exclude=None):
    count_by_status = OrderedCounter()
    for lead in leads:
        if exclude and exclude(lead):
            continue
        count_by_status[lead['status_label']] += 1
    return count_by_status

def segment_statuses_for_leads_by_property(leads, prop_getter=None, exclude=None):
    leads_by_property = defaultdict(list)
    for lead in leads:
        leads_by_property[prop_getter(lead)].append(lead)

    statuses_by_property = OrderedDict()
    for prop, leads in leads_by_property.iteritems():
        statuses_by_property[prop] = statuses_for_leads(leads, exclude=exclude)

    return statuses_by_property

def get_leads_by_start_of_week(number_of_weeks=None):
    has_more = True
    offset = 0
    last_lead = None

    leads_by_start_of_week = defaultdict(list)

    while has_more:
        leads_merged_this_page = 0

        # Get a page of leads
        resp = api.get('lead', data={
            'query': 'sort:date_created',
            '_skip': offset,
            '_fields': 'id,display_name,name,status_label,opportunities,custom,date_created'
        })
        leads = resp['data']

        for lead in leads:
            start_of_week_date = start_of_week(dateutil.parser.parse(lead['date_created']))
            leads_by_start_of_week[start_of_week_date].append(lead)

        # In order to make sure we don't skip any possible duplicates at the per-page boundry, we subtract offset
        # by one each time so there's an overlap. We also subtract the number of leads merged since those no longer exist.
        offset += max(0, len(leads) - 1)
        has_more = resp['has_more']

    ordered = sorted(leads_by_start_of_week.items())
    if number_of_weeks:
        ordered = ordered[-number_of_weeks:]
    return OrderedDict(ordered)

def funnel_at_each_status(lead_count_by_status):
    total_number_of_leads = sum(lead_count_by_status.values())
    remaining_statuses = copy.copy(status_order)
    status_funnel = OrderedCounter()

    while len(remaining_statuses):
        current_status = remaining_statuses[0]
        for status in remaining_statuses:
            status_funnel[current_status] += lead_count_by_status.get(status, 0)
        remaining_statuses.pop(0)

    for status in status_exclude:
        status_funnel[status_order[0]] += lead_count_by_status.get(status, 0)

    percentage_status_funnel = dict()
    for i, status in enumerate(status_order):
        if i == 0:
            percentage_status_funnel[status] = 100.0
        else:
            first = status_funnel[status_order[0]]
            percentage_status_funnel[status] = float(status_funnel[status]) / float(first) * 100


    return dict(
        raw=status_funnel,
        percentage=percentage_status_funnel
    )


def parse_segment_property_to_getter(prop, prop_default=None):
    selectors = prop.split(".")
    def get_prop(lead):
        current = lead
        for selector in selectors:
            current = current.get(unicode(selector))
            if not current:
                current = prop_default
                break
        return current
    return get_prop

class Formatter(object):
    @staticmethod
    def get_formatter(fmt):
        if fmt == "csv":
            return CSVFormatter
        elif fmt == "json":
            return JSONFormatter
        else:
            return PrettyFormatter

    def __init__(self, data, funnel_format):
        self.data = data
        self.funnel_format = funnel_format

    def format_step(self, step):
        if self.funnel_format == "percentage":
            return "{0:.1f}%".format(step)
        else:
            return str(step)

    def output(self):
        pass

class PrettyFormatter(Formatter):

    def output(self):
        for date in self.data.get('dates'):
            headers = ["Step"]
            columns = []
            for segment in self.data.get('segments'):
                funnel_on_date = self.data.get('funnels').get(segment).get(date)
                if funnel_on_date:
                    headers.append(segment)
                    values = []
                    for key in status_order:
                        values.append(self.format_step(funnel_on_date.get(self.funnel_format)[key]))
                    columns.append(values)

            print date
            print "=" * 12
            print tabulate(
                zip(status_order, *columns),
                headers=headers,
                tablefmt='grid'
            )
            print "\n"

class JSONFormatter(Formatter):
    def output(self):
        print json.dumps(data, indent=4, cls=CustomJSONEncoder)

class CSVFormatter(Formatter):
    def output(self):
        print ",".join(["Date"] + self.data.get('dates'))
        for segment in self.data.get('segments'):
            print segment
            columns = []
            for date in self.data.get('dates'):
                funnel_on_date = self.data.get('funnels').get(segment).get(date)
                if funnel_on_date:
                    values = []
                    for key in status_order:
                        values.append(self.format_step(funnel_on_date.get(self.funnel_format)[key]))
                    columns.append(values)
                else:
                    columns.append([self.format_step(0)] * len(status_order))
            zipped = zip(status_order, *columns)
            for item in zipped:
                print ",".join(item)

leads_by_start_of_week = get_leads_by_start_of_week(number_of_weeks=args.weeks)
data = dict(dates=[], segments=OrderedSet(['Total']), funnels=defaultdict(OrderedDict))

for date, leads in leads_by_start_of_week.iteritems():
    date = date.isoformat()
    if args.include_not_native:
        exclude = None
    else:
        exclude = lambda x: x.get("custom").get("Plan") and x.get("custom").get("Plan") != "Native"

    total_statuses =  statuses_for_leads(leads, exclude=exclude)
    data.get('funnels')['Total'][date] = funnel_at_each_status(total_statuses)

    if args.segment:
        segmented_statuses_dict = segment_statuses_for_leads_by_property(
            leads,
            prop_getter=parse_segment_property_to_getter(args.segment, prop_default=args.segment_default_value),
            exclude=exclude
        )

        for segment, totals in segmented_statuses_dict.iteritems():
            data.get('segments').add(segment)
            data.get('funnels')[segment][date] = funnel_at_each_status(totals)

    data.get('dates').append(date)

Formatter.get_formatter(args.format)(data, args.funnel_format).output()
