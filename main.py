import configparser
import pandas as pd
import os, sys, glob
import shutil
from datetime import datetime, date
from collections import defaultdict, OrderedDict
import re
import zipfile
import copy_source_files as source
import controls as ct
import utils as utils
import py7zr
import subprocess
#import histo_pbi as histo


# parameters
Config = configparser.ConfigParser()

Config.read("config.ini")

source_path = Config['PATHS']['source_path']
pbi_path = Config['PATHS']['pbi_path']
results_path = Config['PATHS']['results_path']

intro = """
    
                         Base Collat Production
                         
    
Please find the different programs you may run:

    1. Do you want to copy the production files automatically ?
    
    2. Do you want to copy the production file manually ?
    
        2.1. Please copy the current filename in the prompt
        
        2.2. Please copy the J-1 filename in the prompt
    
    3. Do you want to run all the controls
    
    4. Do you want to save the production in the database ?
    
    
"""

print(intro)

choice = input("Which program do you want to run ? (1, 2 or 3) ")

if choice == str(1):
    source.copy_files_no_existing_files(source_path, pbi_path)
elif choice == str(2):
    source.choose_your_files(source_path, pbi_path)
elif choice == str(3):
    ct.run_controls()
# elif choice == str(3):
    #histo.run_histo_pbi()




