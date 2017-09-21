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


def entries_to_remove(entries, the_dict):
    for keys in entries:
        if keys in the_dict:
            del the_dict[keys]


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

        with open(key, 'rb') as raw:
            raw_reader = csv.DictReader(raw)
            raw_headers = raw_reader.fieldnames

            # if the format is the old 0, 1, 2, 3, 4, 5, n rating scale, convert it differently
            if "0" in raw_headers:
                key_list = ["0", "1", "2", "3", "4", "5"]
                con_headers = [i for i in raw_headers if i not in key_list]
                if 'HMCoord' not in con_headers:
                    con_headers.extend(['HMCoord'])
                    nohm = True
                con_headers.extend(['Drag Copy#', 'Rating'])
                dwriter = csv.DictWriter(convertfile, fieldnames=con_headers)
                dwriter.writeheader()

                for row in raw_reader:
                    countn = 1
                    for k, v in row.items():
                        #  make the HMCoord column based on the Range and pass values
                        if nohm == True:
                            if row['Range'] == '': continue
                            else:
                                if int(row['Range']) < 10 and int(row['Pass']) in range(10, 100):
                                    row['HMCoord'] = str(row['Range'] + '0' + row['Pass'])
                                elif int(row['Range']) >= 10 and int(row['Pass']) < 10:
                                    row['HMCoord'] = str(row['Range'] + '00' + row['Pass'])
                                elif int(row['Range']) < 10 and int(row['Pass']) < 10:
                                    row['HMCoord'] = str(row['Range'] + '00' + row['Pass'])
                                elif int(row['Range']) > 10 and int(row['Pass']) in range(10, 100):
                                    row['HMCoord'] = str(row['Range'] + '0' + row['Pass'])
                                elif int(row['Pass']) > 99:
                                    row['HMCoord'] = str(row['Range'] + row['Pass'])

                        if k in key_list:
                            # get info up to the ratings summaries into the dictionary
                            new_dict = row
                            entries_to_remove(key_list, new_dict)
                            if v == '':
                                continue
                            else:
                                for i in range(int(v)):
                                    new_dict['Drag Copy#'] = countn
                                    new_dict['Rating'] = k
                                    countn += 1
                                    dwriter.writerow(new_dict)

            else:  # the file is in the numbered ratings format and can be dealt with as normal
                writer = csv.writer(convertfile, delimiter=',', lineterminator='\n')
                writer.writerow(converted_header)
                for row in raw_reader:
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
