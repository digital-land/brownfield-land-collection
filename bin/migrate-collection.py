#!/usr/bin/env python3

# migrage collection source and endpoint csv files, adding entry-date from the log.csv, and a source key where missing

import os
import csv
import hashlib
from datetime import datetime

endpoints = {}
endpoint_seen = {}
source_seen = {}


def entry_date(row):
    date = row.get("entry-date", "")
    if not date:
        date = endpoints.get(row["endpoint"], "")[:19] + "Z"
    if not date:
        date = datetime.now().replace(microsecond=0).isoformat() + "Z"
    return date


# get earliest date each endpoint was used ..
for row in csv.DictReader(open("collection/log.csv")):
    if row["endpoint"] not in endpoints:
        endpoints[row["endpoint"]] = row["entry-date"]


#  endpoint
#
w = csv.DictWriter(
    open("collection/-endpoint.csv", "w"),
    [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ],
)
w.writeheader()

for row in csv.DictReader(open("collection/endpoint.csv")):
    row["entry-date"] = entry_date(row)
    w.writerow(row)


#  source
#
w = csv.DictWriter(
    open("collection/-source.csv", "w"),
    [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ],
)
w.writeheader()

for row in csv.DictReader(open("collection/source.csv")):
    if not row.get("source", ""):
        key = "%s|%s|%s" % (row["collection"], row["organisation"], row["endpoint"])
        row["source"] = hashlib.md5(key.encode()).hexdigest()
    row["entry-date"] = entry_date(row)
    w.writerow(row)


if endpoint_seen:
    print("duplicate endpoints:", endpoint_seen)

if source_seen:
    print("duplicate sources:", source_seen)

os.system("mv collection/-endpoint.csv collection/endpoint.csv")
os.system("mv collection/-source.csv collection/source.csv")
