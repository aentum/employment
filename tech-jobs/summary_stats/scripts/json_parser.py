import re
import os
import csv
import pandas as pd
import itertools
import numpy as np
from collections import Counter
import json
import sys

from employee import Employee

negcounts = False
nl = 0
num_amazon = 0
num_fail = 0
garbage = 0

def json_parser(processor, empl_by_year, empl_path, infer_tickers, primary_skills, ai_prop):
    #block for annual counts. 
    def annualCounter(ex, entry):
        ''' Updates empl_by_year dictionary '''
        if ex['identifier'] not in tickers:
            xxx = 0
        else:
            global negcounts
            negcounts = True
        if ex['identifier'] in tickers and ex['start'] != "None" and (ex['end'] != "None" or ex['current_job']):
            empl_by_year[ex['identifier']] += Counter(
                range(
                    pd.to_datetime(ex['start']).year,
                    pd.to_datetime(ex['end']).year if ex['end']!="None" else 2019
                )
            )
            #TODO: Each year needs a list of relevant skillsets
            # Need to extract normalized list of skills from each profile
            # Later will list/graph the top 5~10 skills each year.
            for skill in aiskills:
                is_ai = False
                if skill in entry['primary_skill'] or \
                skill in entry['raw_skills'] or \
                ('bio' in entry and skill in str(entry['bio'])) or \
                skill in ex['role']['valid_roles'] or \
                skill in ex['role']['positions'] or \
                ('description' in ex and skill in str(ex['description'])):
                    is_ai = True
                    break
            if is_ai:
                ai_prop[1][ex['identifier']] += Counter(
                    range(
                        pd.to_datetime(ex['start']).year,
                        pd.to_datetime(ex['end']).year if ex['end']!="None" else 2019
                    )
                )
            else:
                ai_prop[0][ex['identifier']] += Counter(
                    range(
                        pd.to_datetime(ex['start']).year,
                        pd.to_datetime(ex['end']).year if ex['end']!="None" else 2019
                    )
                    )

    def load_and_process(line):
        entry = json.loads(line)
        profile_keys = ['year_of_birth','gender','primary_skill', 
        'secondary_skill', 'country','degrees','elite_edu']
        try:
            profile = {k:entry[k] for k in profile_keys}
        except KeyError as e:
            print(entry)
            print(e)
        #print('----profile----')
        #print(profile)

        if profile['gender'] == 'male':
            profile['gender'] = 2
        elif profile['gender'] == 'female':
            profile['gender'] = 1
        else:
            profile['gender'] = 0

        profile['degrees'] = max(profile['degrees'])
        profile['primary_skill'] = str(profile['primary_skill']['skill'])
        
        #add profile features like this
        #remember to update varlist of main.py
        profile['faculties'] = []
        profile_keys.append('faculties')
        profile['raw_skills'] = entry['raw_skills']
        profile_keys.append('raw_skills')                

        if 'all' in primary_skills and profile['primary_skill'] != '-1':
            pass
        elif exclusive and profile['primary_skill'] not in all_skills_but:
            pass
        elif profile['primary_skill'] in primary_skills:
            pass
        else: #reset without looking at experiences
            processor.employee = Employee()
            return

        for ex in entry['experience']:
            if ex['is_edu']: # education data
                try:
                    for fac in ex['role']['faculties']:
                        if fac not in profile['faculties']:
                            profile['faculties'].append(fac)
                except TypeError as e:
                    pass # TypeError when ex['role'] = None, which we ignore
            else: #employment data
                #filter irregular workers
                if ex['role'] != None:
                    irregular_worker_filter = [re.search(r"(?i)\W{}\W".format(x)," "+ex['role']['original']+" ") 
                                            is None for x in ["intern","internship","trainee","student"]]
                    is_irregular_worker = (sum(irregular_worker_filter) !=4)
                    if is_irregular_worker:
                        continue
                    else:
                        processor.json_read(entry['user_id'], ex, profile)
                        annualCounter(ex, entry)
                elif ex['identifier'] == 'TIME_OFF' : #ex[role] == None 
                        processor.json_read(entry['user_id'], ex, profile)                       

    tickers = processor.tickers
    exclusive = False
    if primary_skills[0][0] == '/':
        exclusive = True
        all_skills_but = [re.sub(re.sub(r'[/()]','', skill)) 
                            for skill in primary_skills]
        all_skills_but.append('-1')
    aiskills = []
    with open('../data/ai_skills.tsv') as fd:
        rd = csv.reader(fd, delimiter= '\t')
        for row in rd:
            aiskills.append(row[0])

    #calling load_and_process on line
    if os.path.isdir(empl_path):
        empl_file_lst = os.listdir(empl_path)
        for empl_file_name in empl_file_lst:
            empl_file = empl_path + '/' +empl_file_name
            if infer_tickers:
                ticker = empl_file_name[:-5].upper()
                processor.change_ticker([ticker])
            with open(empl_file) as f:
                for line in f:
                    global negcounts 
                    negcounts = False
                    global nl 
                    nl +=1
                    load_and_process(line)
                    if negcounts:
                        global num_amazon
                        num_amazon += 1
                    else:
                        entry = json.loads(line)
                        if entry['primary_skill']['skill'] != -1:
                            global num_fail 
                            num_fail += 1
                        elif entry['primary_skill']['skill'] == -1:
                            global garbage
                            garbage += 1
    elif os.path.isfile(empl_path):
        with open(empl_path) as f:
            for line in f:
                load_and_process(line)
    print('employee count:')
    print(num_amazon)
    print(num_fail)
    print(garbage)
    print(nl)