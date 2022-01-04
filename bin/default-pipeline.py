#!/usr/bin/env python3
import csv

w = None

for row in csv.DictReader(open("collection/source.csv", newline="")):
    if not w:
        w = csv.DictWriter(open("/tmp/source.csv", "w"), fieldnames=row.keys())
        w.writeheader()

    if not row["pipelines"]:
        row["pipelines"] = "brownfield-land"

    w.writerow(row)
