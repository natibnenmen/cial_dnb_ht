import zipfile
import sys
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import json
import logging
import os.path
import gc
from pprint import pprint
import traceback
import linecache
from timeit import default_timer as timer
from functools import wraps


# Create a logger to send log messages to stderr and to a log file
stderr_log_level = logging.ERROR
file_log_level = logging.DEBUG
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(stderr_log_level)
stderr_handler.setFormatter(formatter)
file_handler = logging.FileHandler(f'logfile-{datetime.now().strftime("%Y%m%d%H%M%S%f")}.log')
file_handler.setLevel(file_log_level)
file_handler.setFormatter(formatter)
logger.addHandler(stderr_handler)
logger.addHandler(file_handler)


def get_actualsize(input_obj):
    '''
    This function is used to calculate the actual size of an object in memory.
    Since the sys.getsizeof() return only the memory size of the root object, not including the memory size of objects it containes, so for nested objects is is really useless.
    So this function is a workaround to get the actual size of an object.
    '''
    memory_size = 0
    object_ids = set() # since some objects might be referenced more than once, we need to keep track of the objects we already visited
    objects = [input_obj]
    while objects:
        obj_list = []
        for obj in objects:
            if id(obj) not in object_ids:
                object_ids.add(id(obj))
                memory_size += sys.getsizeof(obj)
                obj_list.append(obj)
        objects = gc.get_referents(*obj_list)
    return memory_size


def time_measure(func):
    '''
    This decorator is used to measure the running time of a function.
    '''
    @wraps(func)
    def time_measure_wrapper(*args, **kwargs):
        start_time = timer()
        res = func(*args, **kwargs)
        total_time = timer() - start_time
        logger.info(f'Running time of the function {func.__name__} took {total_time} seconds')
        return res
    return time_measure_wrapper


class LineParser:
    '''
    This class is used to parse a line of the input file and to adapt it to the output format.
    '''

    def __init__(self, raw_line, entity_mapping, parsing_config):
        '''
        This method is used to initialize the class. it takes three arguments:
        - raw_line: the line to parse
        - entity_mapping: a dictionary used to map the entity code to the entity name
        - parsing_config: a dictionary used to define the parsing configuration
        The two last arguments are generad once for all lines by the caller (DataProcessor). It is passed by reference so it is common to all LineParser instances, and doesn't waste memory.
        '''

        self._raw_line = raw_line
        # This is a list of the keys of the dictionary that will be used to calculate the debt_amount
        self._debt_compponent = ['loans', 'participations', 'guarantees_granted', 'other_concepts']
        self._entity_mapping = entity_mapping
        self._parsing_config = parsing_config
        self._parsed_line = {}
        self._debt = {}

        self.parse_line()
        self.adapt_line()


    @property
    def debt(self):
        ''' returns the parsed line as a dictionary'''
        return self._debt


    @property
    def identification_number(self):
        return self._identification_number


    def parse_line(self):

        from_char = 0
        to_char = 0

        for key, value in self._parsing_config.items():
            to_char += value
            self._parsed_line[key] = self._raw_line[from_char:to_char]
            from_char = to_char        


    def adapt_line(self):

        # map entity code to entity name
        self._debt['entity_name'] = self._entity_mapping.get(self._parsed_line['entity_code'], 'Unknown')

        # filter situation per requirements
        self._debt["situation"] = int(self._parsed_line["situation"].strip())
        if self._debt["situation"] == 11:
            self._debt["situation"] = 1
        
        if not 1 <= self._debt["situation"] <= 6:
            self._debt = None
            self._identification_number = None
            return
        else:
            self._debt["situation"] = str(self._debt["situation"])

        # calculate debt_amount        
        self._debt['debt_amount'] = str(sum([float(self._parsed_line[key].replace(',', '.')) for key in self._parsed_line if key in self._debt_compponent]) * 1000)

        # convert YYYYMM to YYYY-MM-DD
        self._debt['information_date'] = self.adapt_date(self._parsed_line['information_date'])

        # save the identification_number
        self._identification_number = self._parsed_line['identification_number']


    @staticmethod
    def adapt_date(yyyymm):
        # This gives the firs day of the month
        date_obj = datetime.strptime(yyyymm, "%Y%m")
        # This gives the common last day of the month, and we move 4 days forward to be sure to be on next month
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

        # statistics data for debuggin purposes
        self._filtered_count = 0
        self._line_count = 0
        self._duplicate_count = 0
        self._parsing_error_count = 0
        self._duplicate_dict = defaultdict(int)
        # end statistics data
        
        # Configration preparations and initializations
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


    @time_measure
    def process_file_naive(self):
        
        with open(f'{self._output_dir}/{self._input_txt_file}', 'r') as input_txt:
            logger.info(f'size of input_txt: {sys.getsizeof(input_txt)}')
            for line in input_txt:                
                self._line_count += 1 # count lines for debugging purposes
                try:
                    line_parser = LineParser(line, self._entity_mapping, self._parsing_config)
                    if line_parser.identification_number == None:
                        self._filtered_count += 1
                        continue

                    self._debt_dict[line_parser.identification_number].append(line_parser.debt)
                    
                    # count duplicate identification_numbers for debugging purposes
                    if len(self._debt_dict[line_parser.identification_number]) > 1:
                        self._duplicate_count += 1
                        self._duplicate_dict[line_parser.identification_number] += 1
                    
                    del line_parser

                except Exception as e:
                    logger.error(f'Error at line {self._line_count}: {e}')
                    self._parsing_error_count += 1
                    continue

        self.print_data()


    def extract_identification_number(self, line):
        ''' Extract the identification_number only from the line'''
        return line[13:24]
    

    @time_measure
    def process_file_mem_save(self, use_offset=True):

        # prepare a dictionary with the line numbers for each identification_number
        with open(f'{self._output_dir}/{self._input_txt_file}', 'r', encoding='utf-8') as input_txt:
            self._id_line_dict = defaultdict(list)
            # using offset rather line index (and do index*line_size) to avoid the case of an error in the line size
            offset = 0
            for ix, line in enumerate(input_txt):                
                self._line_count += 1
                if use_offset:
                    self._id_line_dict[self.extract_identification_number(line)].append(offset)
                    offset += len(line.encode('utf-8'))
                else:
                    self._id_line_dict[self.extract_identification_number(line)].append(ix)

        # processing the file per identification_number
        self._line_count_x = 0
        with open(f'{self._output_dir}/{self._input_txt_file}', 'r', encoding='utf-8') as input_txt:
            for id, lines in self._id_line_dict.items():
                # loop over the lines for each identification_number
                for line_num in lines:
                    if use_offset:
                        input_txt.seek(line_num)
                        line = input_txt.readline()
                    else:
                        line = linecache.getline(f'{self._output_dir}/{self._input_txt_file}', line_num + 1)
                    
                    self._line_count_x += 1
                    try:
                        line_parser = LineParser(line, self._entity_mapping, self._parsing_config)
                        if line_parser.identification_number == None:
                            self._filtered_count += 1
                            continue
                        
                        self._debt_dict[line_parser.identification_number].append(line_parser.debt)
                
                        del line_parser
                        
                    except Exception as e:
                        logger.error(f'traceback.format_exc(): \n {traceback.format_exc()}')
                        logger.error(f'Exception: {e}')
                        logger.error(f'Error at line {self._line_count_x}, line: \n {line}')
                        self._parsing_error_count += 1
                    
                self.print_data()
                self._debt_dict.clear()

                # count duplicate identification_numbers for debugging purposes
                if len(lines) > 1:
                    self._duplicate_count += 1
                    self._duplicate_dict[id] += 1


    def print_data(self):
        for key, value in self._debt_dict.items():
            # The json.dumps is used to make sure double quotes are printed rather than a single quote
            print(f'{{"identification_number": "{key}", "debts": {json.dumps(value)}}}')
    
    
    def print_statistics(self, aux_data=None):
        # Print statistics for debugging and verifications
        # logger.info(f'duplication dict:\n {self._duplicate_dict}.')
        logger.info(f'Found {len(self._duplicate_dict)} duplicates.')
        logger.info(f'Found {len(self._debt_dict)} unique identification numbers.')
        logger.info(f'size of debt dict: {sys.getsizeof(self._debt_dict)}')
        logger.info(f'actual size of debt dict: {get_actualsize(self._debt_dict)}')
        logger.info(f'Filtered {self._filtered_count} lines.')
        logger.info(f'Processed {self._line_count} lines.')
        logger.info(f'Found {self._duplicate_count} duplicates.')
        logger.info(f'Found {self._parsing_error_count} parsing errors.')
        logger.info(f'aux_data: {aux_data}.')


def main():
    use_offset = True
    if len(sys.argv) == 1:
        print('Please provide the input file.')
        exit(1)

    # A cumbersome way to add argumet option to select between seek(the default) and linecache
    if len(sys.argv) == 3:
        if sys.argv[2] not in ['True', 'False']:
            print('Please provide a boolean value, either True or False for the use_offset parameter.')
            exit(1)
        else:
            use_offset = eval(sys.argv[2])
    if len(sys.argv) > 3:
        print('Too many arguments provided.')
        exit(1)

    data_processor = DataProcessor(input_zip_file=sys.argv[1])
    data_processor.process_file_mem_save(use_offset=use_offset)
    data_processor.print_statistics(f'aux_data={use_offset}')

        
if __name__ == '__main__':
    main()

    