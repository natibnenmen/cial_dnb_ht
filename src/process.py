#import argparse
import zipfile
import sys
from parsing_config import parsing_config
import csv
from pprint import pprint, pformat
from datetime import datetime, timedelta

output_dir = 'working_dir'

# todo - this should be adjusted for the test/prod debts.txt
input_txt_file = 'data.txt'

def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input-file', type=str, required=True)
    # args = parser.parse_args()
    # process_file(args.input_file)
    process_file(sys.argv[1])


def parse_line(line):

    from_char = 0
    to_char = 0
    line_dict = {}

    for key, value in parsing_config.items():
        to_char += value
        print(f'key: {key}, value: {value}, from_char: {from_char}, to_char: {to_char}, value: {line[from_char:to_char]}')
        line_dict[key] = line[from_char:to_char]
        from_char = to_char        
    
    return line_dict


def adapt_line(line, entity_mapping):
    line['entity_name'] = entity_mapping[line['entity_code']]
    line.pop('entity_code')

    line['information_date'] = adapt_date(line['information_date'])
    identification_number = line['identification_number']
    line.pop('identification_number')

    debt_compponent = ['loans', 'participations', 'guarantees_granted', 'other_concepts']
    line['debt_amount'] = sum([float(line[key].replace(',', '.')) for key in line if key in debt_compponent]) * 1000

    print(f'x-identification_number: {identification_number} x-n_line: {line}')

    return  identification_number, line


def adapt_date(yyyymm):
    date_obj = datetime.strptime(yyyymm, "%Y%m")
    print(date_obj)
    next_month = date_obj.replace(day=28) + timedelta(days=4)
    print(f'next_month:  {next_month}')
    print(next_month.day)
    last_day_of_month = next_month - timedelta(days=next_month.day)

    return last_day_of_month.strftime("%Y-%m-%d")

def read_entity_mapping_tsv():
    with open('src/entity_mapping.tsv', 'r') as entity_mapping:
        reader = csv.reader(entity_mapping, delimiter='\t')
        entity_mapping_dict = {rows[0]: rows[1] for rows in reader}
        
    return entity_mapping_dict



def process_file(input_file):


    print(f'Processing file: {input_file}')
    with zipfile.ZipFile(input_file, 'r') as input_zip_file:
        input_zip_file.extractall(output_dir)


    entity_mapping =  read_entity_mapping_tsv()
    print(pprint(entity_mapping))
    print(f'entity_mapping["51007"]: {entity_mapping["51007"]}')
    print(f'entity_mapping["72134"]: {entity_mapping["72134"]}')

    with open(f'{output_dir}/{input_txt_file}', 'r') as input_txt:
        for line in input_txt:
            p_line = parse_line(line)
            idn, a_line = adapt_line(p_line, entity_mapping)
            print("A*********************")
            print(f'z-identification_number: {idn}\n z-line:\n{pformat(a_line)}')
            print("B*********************")
            
            break


if __name__ == '__main__':
    main()

    