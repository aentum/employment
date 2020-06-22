import re
import sys
import os
import unicodecsv as csv
import pandas as pd
import itertools
import numpy as np
from collections import Counter
import argparse

from entryProcessor import EntryProcessor
from employee import Employee
from records import Records
from csv_parser  import csv_parser
from json_parser import json_parser


parser = argparse.ArgumentParser(description='Process employment data.')
parser.add_argument('data_source', metavar='SOURCE', type=str,
                    help='Directory containing data')
parser.add_argument('target', metavar='TARGET', type=str,
                    help='name of the csv file to be written under /outputs')
parser.add_argument('--primary_skills', '-ps', default = ['all'],
                    metavar='PRIMARY SKILLS', type=str,
                    nargs = '+', 
                    help='''List of primary skills of interest.
                    By default, all skills are included. 
                    Write '/(skill)' to filter out specific skill(s).
                    ''')
parser.add_argument('--tickers', '-t', nargs = '*', default= True,
                    help='''
                    Option to provide a list of normalized tickers. 
                    By default, the files under directory are expected 
                    to be named as the corresponding tickers of interest.
                   '''
                   )

args = parser.parse_args()

empl_path = args.data_source #directory of files to process
target = args.target #csv file name to write
primary_skills = args.primary_skills
infer_tickers = args.tickers #boolean 
if infer_tickers != True:
    tickers = infer_tickers
    infer_tickers = False
else:
    try:
        empl_file_lst = os.listdir(empl_path)
        tickers = [os.path.splitext(file_name)[0].upper() for file_name in empl_file_lst]
        infer_tickers = True
    except NotADirectoryError:
        print('NOT SUPPORTED: Tickers must be specified to run directly on data file')
        sys.exit(1)
    

## Initialize. Tickers include companies of interest
rec = Records()
employee = Employee() # default empty employee
processor = EntryProcessor(employee, rec, tickers)

empl_by_year = {}
ai_workers = {}
non_ai = {}
yearly_skillsets = {}
for ticker in tickers:
    empl_by_year[ticker] = Counter([])
    ai_workers[ticker] = Counter([])
    non_ai[ticker] = Counter([])
    yearly_skillsets[ticker] = {}
ai_proportions = [non_ai, ai_workers]

if os.path.isdir(empl_path): # Run on a directory of files
    if os.listdir(empl_path)[0].endswith('.csv'):
        csv_parser(processor, empl_by_year, empl_path, infer_tickers, primary_skills)
    else:
        json_parser(processor, empl_by_year, empl_path, infer_tickers, 
        primary_skills, ai_proportions, yearly_skillsets)
else:
    if empl_path.endswith('.csv'): #Run on a single file
        print('Individual file processing supported only on json')
        sys.exit(1)
    else:
        json_parser(processor, empl_by_year, empl_path, infer_tickers, 
        primary_skills, ai_proportions, yearly_skillsets)

#export employment info about individuals. 
empl_changes_lst = rec.output()
varlist = [
    "type","ticker","yrmth", "birth","gender","skill1","skill2","cntry","edu","f_elite",
    "edu_faculty","raw_skills", "job_role","depmt","ind_next","tenure","nprom"
]

empl_changes_df = pd.DataFrame(data=empl_changes_lst,columns=varlist)
empl_changes_df.to_csv(r'../outputs/' + target + '.csv', index= False)

#export employment counts with ai proportions
empl_by_year = pd.DataFrame(empl_by_year).fillna(0).unstack().reset_index()
non_ai = pd.DataFrame(non_ai).fillna(0).unstack().reset_index()
ai_workers = pd.DataFrame(ai_workers).fillna(0).unstack().reset_index()
empl_by_year.columns = ["ticker", "year", "employment"]
non_ai.columns = ["ticker", "year", "non_ai"]
ai_workers.columns = ["ticker", "year", "ai"]

empl_by_year = pd.merge(empl_by_year, non_ai, on = ['ticker', 'year'], how = 'outer')
empl_by_year = pd.merge(empl_by_year, ai_workers, on = ['ticker', 'year'], how = 'outer').fillna(0)

#join yearly skillset composition
yearly_skillsets = pd.DataFrame(yearly_skillsets).unstack().dropna(0).reset_index()
yearly_skillsets.columns = ["ticker", "year", "skill_count"]
empl_by_year = pd.merge(empl_by_year, yearly_skillsets, on = ['ticker', 'year'])
empl_by_year.to_json(r'../outputs/' + target + '_by_year.json')
