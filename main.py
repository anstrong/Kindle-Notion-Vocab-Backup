import argparse
import os
import datetime

from notion.client import NotionClient
import requests

import json
import sqlite3
from sqlite3 import Error

from progress.bar import Bar

# SETUP
def setArgs():
    parser = argparse.ArgumentParser(description='Save Kindle vocabulary to Notion database.')
    parser.add_argument("-w", "--words", help="Parse words only", action="store_true")
    parser.add_argument("-l", "--lookups", help="Parse lookups only", action="store_true")
    parser.add_argument("-db", "--database", nargs='?', type=str, help="Parse a specific database file")

    return parser.parse_args()
args = setArgs()

def setPath():
    KINDLE_PATH = "/Volumes/Kindle/system/vocabulary/vocab.db"
    ARCHIVE_PATH = f"/Users/{os.getenv('SYSTEM_USER')}/Documents/Kindle_Vocabulary_Builder"

    if args.database:
        source = args.database
    else:
        try:
            source = KINDLE_PATH
        except:
            raise FileNotFoundError("Please either provide a path to a vocabulary database or connect your Kindle.")
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"vocab_{date}"
    filepath = f"{ARCHIVE_PATH}/{filename}.db"
    try:
        os.system(f'cp {source} {filepath}')
        print(f"Database from {source} copied to {ARCHIVE_PATH}")
    except:
        print(f"Current database found at {ARCHIVE_PATH}")
    return filepath
db_file = setPath()

def connectDB(path):
   connection = None
   try:
       connection = sqlite3.connect(path)
       print(f"Connection to database {path} was successful")
   except Error as e:
       print(f"The error '{e}' occurred")
   return connection
connection = connectDB(db_file)

# DEFINITION FETCHING
def searchDictionary(word):
    url = "https://twinword-word-graph-dictionary.p.rapidapi.com/definition/"
    querystring = {"entry": word.lower()}
    headers = {
        'x-rapidapi-key': os.getenv('API_KEY'),
        'x-rapidapi-host': "twinword-word-graph-dictionary.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    return json.loads(response.text)

def parseDefinition(data):
    categories = ['noun', 'verb', 'adjective', 'adverb']
    defs = []
    cats = []
    for cat in categories:
        cat_definition = data[cat]
        if len(cat_definition) > 0:
            cats.append(cat)
            defs.append((cat_definition.replace('(nou)', '').replace('(vrb)', '').replace('(adj)', '').replace('(adv)', ''))[1:])
    return {"Parts of Speech": cats, "Definitions": defs}

# NOTION UPLOADING
client = NotionClient(token_v2= os.getenv('TOKEN_V2'))
wordTable = client.get_collection_view(f"https://www.notion.so/{os.getenv('NOTION_USER')}/{os.getenv('WORD_TABLE_ID')}")
lookupTable = client.get_collection_view(f"https://www.notion.so/{os.getenv('NOTION_USER')}/{os.getenv('LOOKUP_TABLE_ID')}")
''' NOTION ARCHITECTURE (abridged)
    wordTable
    - Word [string]
    - Part of Speech [multiselect] (Noun, Verb, Adjective, Adverb, ... )
    - Primary Definition [string]
    - Secondary Definition [string]
    - Category [select] (New, Unknown, Recognizable, Familiar, Known, Not Found)
    - Look-Ups [relation: lookupTable]
    - Ignore [checkbox]

    lookupTable
    - Word [string]
    - Stem [relation: wordTable]
    - Usage [string]
    - Book [string]
    - Author [string]
    - Ignore [checkbox]
'''

def uploadWord(data):
    row = wordTable.collection.add_row()
    row.word = data['Stem']
    if len(data['Details']) is 0:
        row.category = "Not Found"
        row.ignore = True

    elif len(data['Details']['Definitions']) > 0:
        row.category = "New"
        row.ignore = False
        row.primary_definition = data['Details']['Definitions'][0]
        if len(data['Details']['Definitions']) > 1:
            row.secondary_definition = data['Details']['Definitions'][1]
        row.part_of_speech = data['Details']["Parts of Speech"]

def uploadLookup(data):
    row = lookupTable.collection.add_row()
    row.word = data['Word']
    row.usage = data['Usage']
    row.book = data['Book']
    row.author = data['Author']
    row.stem = wordTable.collection.get_rows(search=data['Stem'])[0]

# DATABASE PROCESSING
WORD_DB = "words"
LOOKUP_DB = "lookups"
BOOK_DB = "book_info"

word_issues = []
def getWord(row):
    word = row[1]
    stem = row[2]

    #print(word)

    duplicates = wordTable.collection.get_rows(search=stem)
    if len(duplicates) is 0:

        data = searchDictionary(stem)
        try:
            details = parseDefinition(data['meaning'])
        except:
            details = ""
            word_issues.append(stem)
        entry = {"Word": word, "Stem": stem, "Details": details}
        uploadWord(entry)
        return entry

lookup_issues = []
def getLookup(row):
    word_cur = connection.cursor()
    book_cur = connection.cursor()

    word_key = row[1]
    book_key = row[2]
    usage = row[5]
    word_cur.execute(f"select * from {WORD_DB} where id=:key", {"key": word_key})
    for row in word_cur:
        if str(word_key) is row[0]:
            word = row[1]
            stem = row[2]
        else:
            lookup_issues.append(usage)
            word = ""
            stem = ""
        break
    book_cur.execute(f"select * from {BOOK_DB} where id=:key", {"key": book_key})
    for row in book_cur:
        book = row[4]
        author = row[5]
        break

    filter_params = {
        "filters": [
            {
                "filter": {
                    "value": {
                        "type": "exact",
                        "value": usage
                    },
                    "operator": "string_is"
                },
                "property": "usage"
            }
        ],
        "operator": "and"
    }

    result = lookupTable.build_query(filter=filter_params).execute()
    duplicate_count = 0
    for row in result:
        duplicate_count += 1

    author_temp = author.split(", ")
    author_temp.reverse()
    author_formatted = " ".join(author_temp)
    if duplicate_count is 0:
        #print(word)
        entry = {"Word": word, "Stem": stem, "Usage": usage, "Book": book, "Author": author_formatted}
        uploadLookup(entry)
        return entry

def getData(table, floor = 0, ceiling = 2000, stop =  False):
    cur = connection.cursor()
    cur.execute(f'select * from {table}')

    results = []

    bar = Bar(f'Processing {table}', max=2000)
    i = 0
    for row in cur:
        i += 1
        if (i <= ceiling) and (i > floor):
            if table is "words":
                result = getWord(row)
            elif table is "lookups":
                result = getLookup(row)
            else:
                print(f"Please select valid table")
                break
            if result:
                results.append(result)
        else:
            if stop:
                break
        bar.next()

    print(results)
    return results

# MAIN
def main():
    if args.words and not args.lookups:
        getData(WORD_DB)
    elif args.lookups and not args.words:
        getData(LOOKUP_DB)
    else:
        getData(WORD_DB)
        getData(LOOKUP_DB)

main()

