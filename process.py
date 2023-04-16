import zipfile
import sys
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import json
import logging
import os.path


# Create a logger object
logger = logging.getLogger(os.path.basename(__file__))
#logger.setLevel(logging.INFO)

# Create a handler for stderr
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)  # Set the logging level for stderr

# Create a formatter for the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stderr_handler.setFormatter(formatter)

# Create a handler for the log file
file_handler = logging.FileHandler(f'logfile-{datetime.now().strftime("%Y%m%d%H%M%S%f")}.log')
file_handler.setLevel(logging.ERROR)  # Set the logging level for the log file
file_handler.setFormatter(formatter)

# Add the stderr and file handlers to the logger
logger.addHandler(stderr_handler)
logger.addHandler(file_handler)


class Debt:

    def __init__(self, raw_line, entity_mapping, parsing_config) -> None:

        self._raw_line = raw_line
        self._debt_compponent = ['loans', 'participations', 'guarantees_granted', 'other_concepts']
        self._entity_mapping = entity_mapping
        self._parsing_config = parsing_config
        self._parsed_line = {}
        self._debt = {}

        self.parse_line()
        self.adapt_line()


    @property
    def debt(self):
        return self._debt


    @property
    def identification_number(self):
        return self._identification_number


    def parse_line(self):

        from_char = 0
        to_char = 0

        #for key, value in line_parser_config.items():
        for key, value in self._parsing_config.items():
            to_char += value
            self._parsed_line[key] = self._raw_line[from_char:to_char]
            from_char = to_char        


    def adapt_line(self):

        # map entity code to entity name
        try:
            self._debt['entity_name'] = self._entity_mapping[self._parsed_line['entity_code']]
        except KeyError:
            self._debt['entity_name'] = 'Unknown'

        # filter situation per requirements
        self._debt["situation"] = int(self._parsed_line["situation"].strip())
        if self._debt["situation"] == 11:
            self._debt["situation"] = 1
        
        if not 1 <= self._debt["situation"] <= 6:
            self._debt
            self._identification_number = None
            return
        else:
            self._debt["situation"] = str(self._debt["situation"])

        # calculate debt_amount        
        self._debt['debt_amount'] = str(sum([float(self._parsed_line[key].replace(',', '.')) for key in self._parsed_line if key in self._debt_compponent]) * 1000)

        # convert YYYYMM to YYYY-MM-DD
        self._debt['information_date'] = self.adapt_date(self._parsed_line['information_date'])

        # save identification_number
        self._identification_number = self._parsed_line['identification_number']


    @staticmethod
    def adapt_date(yyyymm):
        date_obj = datetime.strptime(yyyymm, "%Y%m")
        next_month = date_obj.replace(day=28) + timedelta(days=4)
        last_day_of_month = next_month - timedelta(days=next_month.day)

        return last_day_of_month.strftime("%Y-%m-%d")



class DataProcessor:

    def __init__(
            self,
            input_zip_file,
            input_txt_file='data.txt',
            output_dir='working_dir', 
            entity_mapping_file='cfg/entity_mapping.tsv',                       
            parsing_config_file='cfg/parsing_config.json'
        ):

        self._output_dir = output_dir
        self._input_txt_file = input_txt_file
        self._debt_dict = defaultdict(list)

        # statistics data
        self._filtered_count = 0
        self._line_count = 0
        self._duplicate_count = 0
        self._duplicate_dict = defaultdict(int)
        # end statistics data
        
        self.read_entity_mapping_tsv(entity_mapping_file=entity_mapping_file)
        self.read_parsing_config(parsing_config_file=parsing_config_file)
        self.unzip_input_file(input_zip_file=input_zip_file, input_txt_file=input_txt_file)


    def read_parsing_config(self, parsing_config_file):
        with open(parsing_config_file, 'r') as parsing_config:
            self._parsing_config = json.load(parsing_config)


    def read_entity_mapping_tsv(self, entity_mapping_file):
        with open(entity_mapping_file, 'r') as entity_mapping:
            reader = csv.reader(entity_mapping, delimiter='\t')
            self._entity_mapping = {rows[0]: rows[1] for rows in reader}
            logger.info(f'Entity mapping file read with {len(self._entity_mapping)} entries')


    def unzip_input_file(self, input_zip_file, input_txt_file):
        with zipfile.ZipFile(input_zip_file, 'r') as zip_file:
            zip_file.extractall(self._output_dir)


    def process_file(self):
        
        with open(f'{self._output_dir}/{self._input_txt_file}', 'r') as input_txt:
            for line in input_txt:
                self._line_count += 1
                debt = Debt(line, self._entity_mapping, self._parsing_config)
                if debt.identification_number == None:
                    self._filtered_count += 1
                    continue

                self._debt_dict[debt.identification_number].append(debt.debt)
                
                if len(self._debt_dict[debt.identification_number]) > 1:
                    logger.info(f'Found duplicate for {debt.identification_number} at count {self._line_count}')
                    self._duplicate_count += 1
                    self._duplicate_dict[debt.identification_number] += 1
                
                del debt
                #break


    def print_data(self):
        for key, value in self._debt_dict.items():
            print(f'{{"identification_number": "{key}", "debts": {json.dumps(value)}}}')

        logger.info(f'duplication dict:\n {self._duplicate_dict}.')
        logger.info(f'Found {len(self._duplicate_dict)} duplicates.')
        logger.info(f'Found {len(self._debt_dict)} unique identification numbers.')
        logger.info(f'Filtered {self._filtered_count} lines.')
        logger.info(f'Processed {self._line_count} lines.')
        logger.info(f'Found {self._duplicate_count} duplicates.')

                

def main():
    if len(sys.argv) == 1:
        print('Please provide the input file.')
        exit(1)
    data_processor = DataProcessor(input_zip_file=sys.argv[1])
    data_processor.process_file()
    data_processor.print_data()

        
if __name__ == '__main__':
    main()

    