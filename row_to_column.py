import csv
import pandas as pd
import glob
import os
import time

path = r'\\CA016NDOWD001\SaskatoonResearchStation\6. Pathology\Variety and VAT\2017 data conversion'
extension = 'csv'
os.chdir(path)
csv_result = [x for x in glob.glob('*.{}'.format(extension))]

# get the raw data files in a dictionary and match them to their POB counterpart
rawPOB = {}
for csvfiles in csv_result:
    if '_report' in csvfiles:
        continue
    if '_converted' in csvfiles:
        continue
    if 'POB' in csvfiles:
        continue
    else:
        """create a POB file name to merge into"""
        pobFile = csvfiles.replace('.csv', ' POB.csv')
        rawPOB[csvfiles] = pobFile

completed_path = r'\\CA016NDOWD001\SaskatoonResearchStation\6. Pathology\Variety and VAT\2017 data conversion\completed'


def converted_csv(path_file):
    """create a file name for the end resulting csv file:
    :param path_file:
    """
    num = path_file.find('.')
    printname = path_file[:num]
    printname += '_converted.csv'
    return printname


def final_csv(path_file):
    """create a file name for the end resulting csv file:
    :param path_file:
    """
    num = path_file.find('.')
    printname = path_file[:num]
    printname += '_with_rating.csv'
    return printname


def rating_num(rating_file):
    """open a path rating file and determine the amount of ratings were done in the file
    :param rating_file:
    """
    r = open(rating_file, 'rb').read().split('\r\n')

    header_line = ['Entry Book Season', 'Entry Book Project', 'Lab Source Book Name', 'Location', 'Field Book Name']
    for line in r:
        bits = line.split(',')
        if bits[0] in header_line:
            header_ids = bits[:]
            ratings = []
            for info in header_ids:
                try:
                    if int(info) in range(1, 200):
                        ratings.append(info)
                except:
                    continue

            return ratings


def header_id(raw_data):
    """open the raw data file and find only sample info headers, excluding the columns of ratings
    :param raw_data:
    """
    with open(raw_data, 'rb') as con_file:
        con_reader = csv.DictReader(con_file)
        con_headers = con_reader.fieldnames

        header_info = []
        for column in con_headers:
            try:
                column = int(column)
            except:
                if column == "":
                    continue
                else:
                    header_info.append(column)

        return header_info


def similar_headers(converted_data, pob_data):
    with open(converted_data, 'rb') as conv:
        reader1 = csv.DictReader(conv)
        conv_headers = reader1.fieldnames
        with open(pob_data, 'rb') as po:
            reader2 = csv.DictReader(po)
            pob_header = reader2.fieldnames

            sim_head = []
            for iz in conv_headers:
                if iz in pob_header:
                    if iz == 'Name':
                        continue
                    else:
                        sim_head.append(iz)
                else:
                    continue
            sim_head.append('Rating')
            return sim_head

for key, val in rawPOB.items():
    print 'Merging raw datafile: {} with POB Book: {} \n'.format(key, val)

    conv_file = converted_csv(key)  # get a converted file name
    rating_final = final_csv(val)  # give a final naming convention to the file you create
    rate_list = rating_num(key)  # get the list of ratings in the original raw data file
    original_headers = header_id(key)  # grab the header info from the raw data file
    converted_header = header_id(key)  # make a converted header list variable
    converted_header.extend(['Drag Copy#', 'Rating'])  # add the missing columns required on converted file

    """open the raw_datafile and write a new file that converts the ratings to a row by row format, saving into a new
    file named x_converted"""

    with open(conv_file, 'wb') as convertfile:
        writer = csv.writer(convertfile, delimiter=',', lineterminator='\n')
        writer.writerow(converted_header)

        with open(key, 'rb') as raw:
            raw_reader = csv.DictReader(raw)
            raw_headers = raw_reader.fieldnames

            for row in raw_reader:  # read the file line by line
                drag_copy = 1
                for number in rate_list:
                    new_line = [row[i] for i in original_headers]
                    new_line.append(drag_copy)
                    new_line.append(row[number])
                    drag_copy += 1
                    writer.writerow(new_line)

    with open(val, 'rb') as pob:
        reader = csv.DictReader(pob)
        pob_headers = reader.fieldnames

    # using pandas, make a dataframe from the converted file that only has the matching header names to the POB

    pre_merge_headers = similar_headers(conv_file, val)

    # pre_merge_headers = ['Entry #', 'Geno_Id', 'Local Range', 'Plot #', 'Drag Copy#', 'Rating']

    if 'Comments' in pre_merge_headers:
        pre_merge_headers.remove('Comments')

    df = pd.read_csv(conv_file)
    mdf = df[pre_merge_headers]
    projectbook_df = pd.read_csv(val, keep_default_na=False, na_values=[""])

    merged_left = pd.merge(left=projectbook_df, right=mdf, how='left')
    os.remove(conv_file)
    os.remove(key)
    os.remove(val)

    os.chdir(completed_path)
    merged_left.to_csv(rating_final, index=False)

    time.sleep(1)

    print "Merge completed.  {} moved to completed folder \n". format(val)

print "Completed analysis on all files in folder"

time.sleep(2)
