#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import sys
from pymongo import MongoClient


def main():
    # set mongodb info
    mongodb_url = "mongodb://localhost/"
    mongodb_name = "mozart"
    stdouter_col = "stdouterr"

    # get mongodb collection for job_status
    client = MongoClient(mongodb_url)
    db = client[mongodb_name]
    col = db[stdouter_col]

    # get all job id's stdouterr chunks
    recs = col.find({"id": sys.argv[1]}, {"chunk": True, "datetime": True}).sort(
        "datetime"
    )

    # print chunks
    for rec in recs:
        print(rec["chunk"].strip())


if __name__ == "__main__":
    main()
