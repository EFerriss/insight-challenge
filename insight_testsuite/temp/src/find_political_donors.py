# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 10:16:34 2017

@author: Elizabeth Ferriss
"""

from timeit import default_timer as timer
from decimal import Decimal, ROUND_HALF_UP
from statistics import median
import sys

cmte_ids = []
zip_codes = []
dates = []
transaction_amts = []
zip_outputs = None
date_outputs = None

x = 0
# process input as if sequentially streaming, i.e., one line at a time
# consider making that bit its own function and writing a tests for each
with open(sys.argv[1]) as data_file:
    for line in data_file:
        new_data = line.split("|")
        
        # data dictionary on fec.gov
        cmte_id = new_data[0]
        zip_code = new_data[10][:5]
        date = new_data[13]
        transaction_amt = int(new_data[14]) 
        other_id = new_data[15]
        
        # only include data where OTHER_ID is empty and ignore lines
        # without cmte_id (the recipient) or transation amount
        if (other_id) or (not cmte_id) or (not transaction_amt):
            continue

        # This strategy will eventually run out of memory!
        # look into defaultdicts for keeping track of running medium...
        cmte_ids.append(cmte_id)
        zip_codes.append(zip_code)
        dates.append(date)
        transaction_amts.append(transaction_amt)

        # prepare output for medianvals_by_zip.txt
        # exclude data from malformed zip codes
        if len(zip_code) == 5:
            # running count of money by recipient and zip code 
            amts_from_zipcode = [money for money, zipc, person
                                 in zip(transaction_amts, zip_codes, cmte_ids)
                                 if zipc == zip_code
                                 if person == cmte_id]
            
            ncontributions = len(amts_from_zipcode)
            
            # median - drop anything below $.50 and round up $.50 or higher
            median_amt = median(amts_from_zipcode)
            median_amt = Decimal(median_amt).quantize(0, ROUND_HALF_UP)
                   
            # it's faster to save to the file all at once than after each line
            zip_output = '|'.join((cmte_id, 
                                   zip_code, 
                                   str(median_amt),
                                   str(ncontributions),
                                   str(sum(amts_from_zipcode))))
            try:
                zip_outputs = '\n'.join((zip_outputs, zip_output))
            except TypeError:
                zip_outputs = zip_output
                
        x = x+1
        if x > 1000:
            break

## prepare output for medianvals_by_date.txt
unique_recipients = sorted(set(cmte_ids))
for person in unique_recipients:
    # dates person recieved money in chronological order
    dmoney = set(date for date, per in zip(dates, cmte_ids) if per == person)
    dmoney = sorted(dmoney, key=lambda x: x[4:]+x[0:4])
    
    # get values for each date and store
    for date in dmoney:
        amts_on_date = [money for money, day in zip(transaction_amts, dates)
                        if day == date]
        
        median_amt = median(amts_on_date)
        median_amt = Decimal(median_amt).quantize(0, ROUND_HALF_UP)
        ncontributions = len(amts_on_date)
    
        date_output = '|'.join((person, 
                                date, 
                                str(median_amt),
                                str(ncontributions),
                                str(round(sum(amts_on_date)))))
        try:
            date_outputs = '\n'.join((date_outputs, date_output))
        except TypeError:
            date_outputs = date_output

# save to files
with open(sys.argv[2], "w") as zipcode_file:
    zipcode_file.write(zip_outputs)
    
with open(sys.argv[3], "w") as date_file:
    date_file.write(date_outputs)

