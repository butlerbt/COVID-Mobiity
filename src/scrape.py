"""
Author: Brent Butler
Purpose: Download Google mobility data which is provided in the inconvenient pdf format, parse the pdfs, and export data 
         in csv format.
Date: 2020-04-11
Contact: butlerbt.mg@gmail.com
"""

import pdfminer
import io
import os
import shutil
import requests
from bs4 import BeautifulSoup

import urllib.request
from collections import OrderedDict

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser

import pandas as pd
import re

def scrape_covid_mobility():
    
    """
    Checks for recent google mobility data and if new data available downloads pdf.
    Saves all pdfs in '../data/raw' directory under sub directory with date of report
    
    outputs: boolean: True if new files downloaded
            directory: directory path where new files were saved 
            
    
    
    """
    
    if not os.path.exists('../data'):
        os.makedirs('../data')
    
    if not os.path.exists('../data/raw'):
        os.makedirs('../data/raw')
        
    #use requests to get the mobility site
    url = 'https://www.google.com/covid19/mobility/'
    response = requests.get(url)
    
    #Use Beautiful Soup to parse the site for html 
    soup = BeautifulSoup(response.text, "html.parser")

    #find the download links for each country and region
    html = soup.findAll('a', {"class":"download-link"})
    
    new_files = False
    
    #check the latest date of links
    date_index = html[0]['href'].find('2020')
    date = html[0]['href'][date_index:date_index+10]
    if not os.path.exists(f'../data/raw/{date}'):
        os.makedirs(f'../data/raw/{date}')
        
    #download all PDFs
    for tag in html:
        link = tag['href']
        file_name = link[link.find('2020'):]      #file name based on download url which always starts with 2020 date
        path = f"../data/raw/{date}/{file_name}"
        #check to see if Google has uploaded new data
        if not os.path.isfile(path):
            new_files = True
            urllib.request.urlretrieve(link, path)
            print(f'new file found: {file_name}')
            
    directory = f'../data/raw/{date}'
    if new_files == True:
        directory = f'../data/raw/{date}'
        print(f'New files downloaded for {date}')
        status = True
        return status, directory
    
    if new_files == False:
        print('No new files')
        status = False
        return status, directory
        
    
def covid_report_to_text(pdf_path):
    """
    takes pdf and extracts text into string
    pdf_path: file path of the pdf to be converted
    returns: string of text
    """ 
    output_string = io.StringIO()
    rsrcmgr = PDFResourceManager()
    device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    with open(pdf_path, 'rb') as pdf_file:
        for page in PDFPage.get_pages(pdf_file):
            interpreter.process_page(page)

        text = output_string.getvalue()

    device.close()
    output_string.close()
    
    return text

def parse_main_region(text):
    """
    Parses pdf text to find stats for macro region (country level
    or state level)
    
    text input is pdf converted into string
    returns: dictionary of data, index of last category scraped
    
    """
    
    data = OrderedDict()
    categories=['Retail & recreation', 
                'Grocery & pharmacy',
                'Parks', 'Transit stations',
                'Workplaces', 'Residential']

    #find macro area and macro level stats
    country_state = text.split('\n\n')[1].split('  ')[0]
    data['Region']=[country_state]
    for cat in categories:
        index = text.find(cat)+len(cat)
        if text[index]!=' ':
            data[cat] = data.get(cat,[])+[int(text[index:index+text[index:].find('%')])]
        else:                                   
            data[cat] = data.get(cat, []) + [None]

    last_cat_index = text.find(categories[-1])
    
    return data, last_cat_index
    

def parse_sub_regions(text, data, last_cat_index):
    """
    Parses pulls out stats for subregions
    
    Takes: text = string converted pdf
            data = ordered dictionary from parse_main_region()
            index = ending index number from parse_main_region()
    
    returns: ordered dictionary with main region and sub region stats
    """
    test = text.find('Retail', last_cat_index) #counter to find the end of the sub regions
    while test >0:
    
        #find the sub region based on location of "Retail" and last cursor location
        region_end_index = text.find('Retail', last_cat_index)-2
        text_slice = text[:region_end_index]
        region_beg_index = text_slice.rfind("\n\n")+2
        region = text[region_beg_index:region_end_index].strip('\x0c')
        data['Region']+=[region]

        cursor = region_end_index #set cursor for categories withing subregion

        for i in categories: #find stats for each category

            if i == 'Transit stations': #exception due to pattern with baseline
                Res_ind = text.find('Residential', cursor)
                percent_ind = Res_ind + 19

                if text[percent_ind]=='%':

                    percent = text[percent_ind-3:percent_ind].strip('\n').strip('%').strip(' ')
                    percent = int(percent)
                    data[i]+=[percent]
                #because moving forward indexed from 'Residential' can be either 
                #18 or 19 spaces, need to check both to see if they contain '%' 
                elif text[percent_ind-1]=='%':
                    percent_ind = percent_ind-1
                    percent = text[percent_ind-3:percent_ind].strip('\n').strip('%').strip(' ')
                    percent = int(percent)
                    data[i]+=[percent]


                else: data[i]+=[None]
                cursor = percent_ind+22


            else:    
                #find the stats based on the % relative locatin from 'line'
                comp_ind = text.find('line', cursor) 
                percent_ind = comp_ind-19

                if text[percent_ind]=='%': #test if category has data 
                    percent = text[percent_ind-3:percent_ind].strip('\n')
                    percent = int(percent)
                    data[i]+=[percent]

                else: data[i]+=[None] #assign none type if data is missing
                cursor = comp_ind+1

        last_cat_index = cursor
        test = text.find('Retail', last_cat_index) #counter to find the end of the sub regions

    return data
    
    #turn dictionary into pandas dataframe 

def dict_to_masterdf(master_df, data):
    """
    Convert dictionary data into dataframe 

    Inputs: master_df : empty df or df to be concatted to
            data: mobility stats in ordered dict in format returned by scrape.parse_sub_regions or scrape.parse_main_region

    Returns: df 
    """
    temp_df = pd.DataFrame(data)
    master_df = master_df.append(temp_df)
    return master_df

def df_to_csv(df, file_name, directory):
    """
    Exports df to csv in appropriate directories

    Input: df: containing all parsed stats
            file_name: string for csv identification, ex: "World", "United States region" 
            directory: directory of pdfs 
    
    output: csv: ../data/processed/{processed_date}/{file_name}_{processed_date}.csv
    """
    processed_date = directory[-10:]
    if not os.path.exists(f'../data/processed/{processed_date}'):
        os.makedirs(f'../data/processed/{processed_date}')
    df.to_csv (f'../data/processed/{processed_date}/{file_name}_{processed_date}.csv', index = False, header=True)

def region_dict_to_masterdf(master_df, data):
    """
    Drops the state level data so every entry is county level
    
    Input: master_df: likely blank df
            data: ordered dict of parsed stats
            
    Returns: df with every state's data appended 
    """
    if len(data['Region'])>1:
        temp_df = pd.DataFrame(data)
        temp_df['State']=data['Region'][0]
        temp_df.drop(index=0, inplace = True)
        master_df = master_df.append(temp_df)
        return master_df
    else:
        temp_df = pd.DataFrame(data)
        temp_df['State']=data['Region'][0]
        master_df = master_df.append(temp_df)
        return master_df

        
    
    
def build_US_state_report(directory):
    """
    Produces csv of county level data of all US States.
    Input: directory path of pdf files
    """
    
    print('Building US county level report')
    us_list = [file for file in os.listdir(directory) if '_US_' in file]
    us_list.remove(f'{us_list[0][:10]}_US_Mobility_Report_en.pdf') #drop the nation wide stats
    master_df = pd.DataFrame()
    for file in us_list:
        text = covid_report_to_text(f'{directory}/{file}')
        data, last_cat_index = parse_main_region(text)
        data = parse_sub_regions(text, data, last_cat_index)
        master_df = region_dict_to_masterdf(master_df, data)
    df_to_csv(master_df, 'United_States_county', directory)
    
    print('US county level report done')
    
def build_global_covid_report(directory):
    """
    Produces csv of nation level data for world.
    Input: directory path of pdf files
    """
    print('Building global report')
    world_list = [file for file in os.listdir(directory) if '_US_' not in file]
    for file in os.listdir(directory):
        if file.endswith("_US_Mobility_Report_en.pdf"):
            world_list.append(file)
    master_df = pd.DataFrame()
    for file in world_list:
        text = covid_report_to_text(f'{directory}/{file}')
        data, last_cat_index = parse_main_region(text)
        master_df = dict_to_masterdf(master_df, data)
    df_to_csv(master_df, 'World', directory)
    print('Global report done')


def build_regionlevel_covid_report(directory):
    """
    Produces individual csvs of region level data for any country with sub region data available.
    Input: directory path of pdf files
    """
    
    print('Building region level report')
    world_list = [file for file in os.listdir(directory) if '_US_' not in file] #filter out US county level data
    for file in os.listdir(directory):
        if file.endswith("_US_Mobility_Report_en.pdf"): #put US state level data back in
            world_list.append(file)
    
    
    for file in world_list:
        text = covid_report_to_text(f'{directory}/{file}')
        data, last_cat_index = parse_main_region(text)
        data = parse_sub_regions(text, data, last_cat_index)
        if len(data['Region'])>1:
            df = pd.DataFrame()
            df = region_dict_to_masterdf(df, data)
            df_to_csv(df, data['Region'][0].replace(' ','_'), directory)
    print('region level done')


def run():
    categories=['Retail & recreation', 
                'Grocery & pharmacy',
                'Parks', 'Transit stations',
                'Workplaces', 'Residential']
    status, directory = scrape_covid_mobility()
    if status == True:
        build_US_state_report(directory)
        build_global_covid_report(directory)
        build_regionlevel_covid_report(directory)
    
        
    
if __name__ == '__main__':
    """
    If script loaded as main program, run the script
    if script loades as module of another program, functions can be called independently
    """
    run()
        

