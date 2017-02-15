import csv
import pandas as pd

raw_datafile = 'A18,2012 ML complete&summ Field Ratingsl.csv'
projectbook = '2012 Moon Lake SK 3A0123 POB.csv'


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
        for ix in con_headers:
            try:
                ix = int(ix)
            except:
                if ix == "":
                    continue
                else:
                    header_info.append(ix)

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


conv_file = converted_csv(raw_datafile)  # get a converted file name
rating_final = final_csv(projectbook)  # give a final naming convention to the file you create
rate_list = rating_num(raw_datafile)  # get the list of ratings in the original raw data file
original_headers = header_id(raw_datafile)  # grab the header info from the raw data file
converted_header = header_id(raw_datafile)  # make a converted header list variable
converted_header.extend(['Drag Copy#', 'Rating'])  # add the missing columns required on converted file

"""open the raw_datafile and write a new file that converts the ratings to a row by row format, saving into a new
file named x_converted"""

with open(conv_file, 'wb') as convertfile:
    writer = csv.writer(convertfile, delimiter=',', lineterminator='\n')
    writer.writerow(converted_header)

    with open(raw_datafile, 'rb') as raw:
        raw_reader = csv.DictReader(raw)
        raw_headers = raw_reader.fieldnames
        print raw_headers
        print rate_list

        for row in raw_reader:  # read the file line by line
            drag_copy = 1
            for number in rate_list:
                new_line = [row[i] for i in original_headers]
                new_line.append(drag_copy)
                new_line.append(row[number])
                drag_copy += 1
                writer.writerow(new_line)

with open(projectbook, 'rb') as pob:
    reader = csv.DictReader(pob)
    pob_headers = reader.fieldnames

# using pandas, make a dataframe from the converted file that only has the matching header names to the POB

pre_merge_headers = similar_headers(conv_file, projectbook)


# pre_merge_headers = ['Entry #', 'Geno_Id', 'Local Range', 'Plot #', 'Drag Copy#', 'Rating'] # use for Viterra NB data
print pre_merge_headers

df = pd.read_csv(conv_file)
mdf = df[pre_merge_headers]
projectbook_df = pd.read_csv(projectbook, keep_default_na=False, na_values=[""])

merged_left = pd.merge(left=projectbook_df, right=mdf, how='left')

merged_left.to_csv(rating_final, index=False)