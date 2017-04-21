#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a simple script to convert json data fields exported from a Kazoo
CouchDB instance into correct datetime and number formats and then import
into a MongoDB instance

Example:
    $ ./cleancdrs.py -if ../mydata.json -cs "mongodb://localhost:27017" \
      -db mydatabase -col mycollection

    If you are on a *ix system, you can use "time" to evaluate running time
    $ time ./cleancdrs.py -if ../mydata.json -cs "mongodb://localhost:27017" \
      -db mydatabase -col mycollection

Todo:
    1. Load the json data in chunks
    2. Add tests
"""

import json
import argparse
import sys
import logging
from logging import info
import re
from email.utils import parsedate_tz, mktime_tz
import datetime
from pymongo import MongoClient

REGEX_FLOAT = r"\d+\.\d+"

def parseargs(parser):
    """Parse args and return filename param

    :param value: parser object
    :return: file object
    """

    parser.add_argument("-if", "--inputfile", help="file name to parse")
    parser.add_argument("-cs", "--connectionstring", help="mongodb connection \
                                string (host/port only)")
    parser.add_argument("-db", "--database", help="database name")
    parser.add_argument("-col", "--collection", help="collection name")
    args = parser.parse_args()
    return args.inputfile, args.connectionstring, args.database, args.collection

def makedatetime(field):
    """Converts to datetime

    :param value: the field to parse
    :return: datetime
    """
    field = makeclean(field)
    if field:
        #attempt parsing rfc_1036
        #"Tue, 28 Feb 2017 23:44:01 GMT"
        try:
            date_tuple = parsedate_tz(field.strip())
            if date_tuple is not None:
                if date_tuple:
                    datetime_object = datetime.datetime.fromtimestamp(mktime_tz(date_tuple))
                    return datetime_object
        except ValueError:
            pass

        try:
            #attempt parsing iso_8601
            #"2017-02-28"
            datetime_object = datetime.datetime.strptime(field, '%Y-%m-%d')
            if datetime_object is not None:
                return datetime_object
        except ValueError:
            pass

        try:
            #attempt parsing kazoo datetime
            #"datetime": "2017-02-28 23:44:01"
            datetime_object = datetime.datetime.strptime(field, '%Y-%m-%d %H:%M:%S')
            if datetime_object is not None:
                return datetime_object
        except ValueError:
            pass


def makenumber(field):
    """Converts to float or int, as appropriate

   :param value: the field to parse
   :return: float or int
    """

    if field:
        if (isinstance(field, int) or isinstance(field, float)) is True:
            return field

        if isinstance(field, str) is False and \
            isinstance(field, unicode) is False:
            raise TypeError(str(field) + " has invalid type (" + type(field) + ")")

        field = makeclean(field)
        field = field.strip()

        if len(field) != 0:
            result = re.search(REGEX_FLOAT, field, flags=re.IGNORECASE)
            if result is not None:
                return float(field)
            else:
                return int(field)
        else:
            return 0

def makeclean(field):
    """Removes unnecessary chars like double-quotes and leading
    and trailing spaces

    :param value: the field to parse
    :return: field cleaned up
    """

    field = field.replace("\"", "").strip()
    return field

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    PARSER = argparse.ArgumentParser()
    IFILE, CONNECTIONSTRING, DATABASE, COLLECTION = parseargs(PARSER)
    if None in (IFILE, CONNECTIONSTRING, DATABASE, COLLECTION):
        PARSER.print_help()
        sys.exit()

    CLIENT = MongoClient(CONNECTIONSTRING)

    with open(IFILE) as cdr_file:
        info("Loading json from: " + IFILE)
        CDR_DATA = json.load(cdr_file)
        TOTAL = len(CDR_DATA["data"])
        counter = 0
        percentage_printed = 0
        for data in CDR_DATA["data"]:

            data["duration_seconds"] = makenumber(data["duration_seconds"])
            data["billing_seconds"] = makenumber(data["billing_seconds"])
            data["cost"] = makenumber(data["cost"])
            data["datetime"] = makedatetime(data["datetime"])
            data["rfc_1036"] = makedatetime(data["rfc_1036"])
            data["iso_8601"] = makedatetime(data["iso_8601"])
            data["rate"] = makenumber(data["rate"])
            data["reseller_cost"] = makenumber(data["reseller_cost"])

            CLIENT[DATABASE][COLLECTION].insert(data)

            counter += 1
            percentage = int(float(counter)/float(TOTAL)*100)
            if percentage > percentage_printed:
                info("Processed = " + str(percentage) + "%")
                percentage_printed = percentage
