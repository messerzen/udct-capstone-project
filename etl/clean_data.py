from process_all import extract_transform_fact_table
from process_establishments import process_stablishments
from process_companies import process_companies

import configparser

def main():

   extract_transform_fact_table()
   #config = configparser.ConfigParser()
   #config.read('aws_paths.cfg')

   #STABLISHMENTS_RAW = config['S3']['STABLISHMENTS_RAW']
   #STABLISHMENTS_CLEANED = config['S3']['STABLISHMENTS_CLEANED']

   #COMPANIES_RAW = config['S3']['COMPANIES_RAW']
   #COMPANIES_CLEANED = config['S3']['COMPANIES_CLEANED']

   #process_stablishments(STABLISHMENTS_RAW, STABLISHMENTS_CLEANED)
   ##process_companies(COMPANIES_RAW, COMPANIES_CLEANED)

if __name__ == '__main__':
    main()