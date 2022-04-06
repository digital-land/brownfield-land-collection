#!/usr/bin/python3

# create a prototype datapackage, fixing up the brownfield-land

import re
import csv
import json

organisations = {}

for row in csv.DictReader(open("var/cache/organisation.csv")):
    organisations[row["entity"]] = row

fieldnames = [
    "entity",
    "prefix",
    "organisation",
    "organisation-name",
    "organisation-entity",
    "reference",
    "entry-date",
    "start-date",
    "end-date",
    "longitude",
    "latitude",
    "hectares",
    "minimum-net-dwellings",
    "maximum-net-dwellings",
    "deliverable",
    "hazardous-substances",
    "ownership-status",
    "planning-permission-date",
    "planning-permission-type",
    "planning-permission-status",
    "planning-permission-history",
    "site-plan-url",
    "site-address",
    "notes",
]

rows = {}
point = re.compile(r"POINT\((?P<longitude>-?\d+\.\d+) (?P<latitude>-?\d+\.\d+)\)")

for row in csv.DictReader(open("dataset/brownfield-land.csv")):
    _row = {}
    for field, value in row.items():
        _row[field.replace("_", "-")] = value
    row = _row

    if row["json"]:
        data = json.loads(row["json"])
        for field, value in data.items():
            row[field] = value
        del row["json"]

    # hack in a prefix for now ..
    row["prefix"] = "bfl" + row["organisation-entity"]

    # split point
    row["longitude"] = row["latitude"] = ""
    if row["point"]:
        match = point.match(row["point"])
        for field in ["longitude", "latitude"]:
            row[field] = match.group(field)

    if row["organisation-entity"]:
        o = organisations[row["organisation-entity"]]
        row["organisation"] = o["organisation"]
        row["organisation-name"] = o["name"]

    rows[row["prefix"] + row["reference"] + row["entity"]] = row


with open("dataset/brownfield-land-package.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames, extrasaction="ignore")
    w.writeheader()
    for key, row in sorted(rows.items()):
        w.writerow(row)
