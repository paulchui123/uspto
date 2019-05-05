# USPTOProcessLinks.py
# USPTO Bulk Data Parser - Processes for Finding Links and Downloading Data Files
# Description: Processes links to data files.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# Import Python Modules
import time
import re
import os
import sys
import traceback
import urllib
from bs4 import BeautifulSoup

# Import USPTO Parser Functions
import USPTOLogger
import USPTOProcessXMLGrant
import USPTOProcessAPSGrant
import USPTOProcessXMLApplication
import USPTOExtractXML4Grant
import USPTOExtractXML2Grant
import USPTOExtractXML4Application
import USPTOExtractXML1Application


# Function to accept raw xml data and route to the appropriate function to parse
# either grant, application or PAIR data.
def extract_data_router(xml_data_string, args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    try:
        if args_array['uspto_xml_format'] == "gAPS":
            return extract_APS_grant(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "gXML2":
            return USPTOExtractXML2Grant.extract_XML2_grant(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "gXML4":
            return USPTOExtractXML4Grant.extract_XML4_grant(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "aXML1":
            return USPTOExtractXML1Application.extract_XML1_application(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "aXML4":
            return USPTOExtractXML4Application.extract_XML4_application(xml_data_string, args_array)
    except Exception as e:
        # Print and log general fail comment
        print "xml extraction failed for document type: " + args_array['uspto_xml_format'] + " link: " + args_array['url_link']
        logger.error("xml extraction for document type: " + args_array['uspto_xml_format'] + " link: " + args_array['url_link'])
        # Print traceback
        traceback.print_exc()
        # Print exception information to file
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


# This function accepts the filename and returns the file format code
def return_file_format_from_filename(file_name):

    # Declare XML type strings for regex
    format_types = {
        "gXML4":'ipgb.*.zip',
        "gXML2":'pgb.*.zip',
        "gXML2_4" : 'pgb2001.*.zip',
        "gAPS" : '[0-9]{4}.zip|pba.*.zip',
        "aXML4" : 'ipab.*.zip',
        "aXML1" : 'pab.*.zip'
    }

    # Check filetype and return value
    for key, value in format_types.items():
        if re.compile(value).match(file_name):
            return key

# Download a link into temporary memory and return filename
def download_zip_file(args_array):

    # Set process start time
    start_time = time.time()

    # Try to download the zip file to temporary location
    try:

        # If Sandbox mode
        if args_array['sandbox']:

            # Strip the file from the url_link
            file_name = args_array['url_link'].split("/")[-1]

            # Check if the file is in the downloads folder first
            if os.path.isfile(args_array['sandbox_downloads_dirpath'] + file_name):
                # Download the file and use system temp directory
                print '[Using previosly downloaded .zip file: {0}]'.format(args_array['sandbox_downloads_dirpath'] + file_name)
                # Use the previously downloaded file as the temp_zip filename
                return args_array['sandbox_downloads_dirpath'] + file_name
            else:
                # Download the file and use system temp directory
                print '[Downloading .zip file to sandbox directory: {0}]'.format(args_array['sandbox_downloads_dirpath'] + file_name)
                file_name = urllib.urlretrieve(args_array['url_link'], args_array['sandbox_downloads_dirpath'] + file_name)[0]
                print '[Downloaded .zip file: {0} Time:{1} Finish Time: {2}]'.format(file_name,time.time()-start_time, time.strftime("%c"))

        # If not sandbox mode
        else:
            # Download the file and use system temp directory
            print '[Downloading .zip file: {0}]'.format(args_array['url_link'])
            file_name = urllib.urlretrieve(args_array['url_link'])[0]
            print '[Downloaded .zip file: {0} Time:{1} Finish Time: {2}]'.format(file_name,time.time()-start_time, time.strftime("%c"))


        # Return the filename
        return file_name
    except Exception as e:
        print 'Downloading  contents of ' + args_array['url_link'] + ' failed...'
        traceback.print_exc()


# Function to route the extraction of raw data from a link
def process_link_file(args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Download the file and append temp location to args array
    args_array['temp_zip_file_name'] = download_zip_file(args_array)

    #print args_array['uspto_xml_format']

    # Process the contents of file baed on type
    if args_array['uspto_xml_format'] == "gAPS":
        USPTOProcessAPSGrant.process_APS_grant_content(args_array)
    elif args_array['uspto_xml_format'] == "aXML1" or args_array['uspto_xml_format'] == "aXML4":
        USPTOProcessXMLApplication.process_XML_application_content(args_array)
    elif args_array['uspto_xml_format'] == "gXML2" or args_array['uspto_xml_format'] == "gXML4":
        USPTOProcessXMLGrant.process_XML_grant_content(args_array)

    print "Finished the data storage process for contents of: " + args_array['url_link'] + " Finished at: " + time.strftime("%c")


# get all the formats of grants and publications
def get_all_links(args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # returns a list
    # PG = Patent Grants
    # PA = Patent Applications

    # Patent Grant Information Retrieval
    url_source_USPTO = 'https://bulkdata.uspto.gov/'
    url_source_UPC_class = "https://www.uspto.gov/web/patents/classification/selectnumwithtitle.htm"

    # TODO: fix the class parser
    print 'Started grabbing patent classification links... ' + time.strftime("%c")
    classification_linklist = []
    classification_linklist.append([args_array['classification_text_filename'], "None"])
    print 'Finished grabbing patent classification links... ' + time.strftime("%c")
    # Log finished building all zip filepaths
    logger.info('Finished grabbing patent classification bibliographic links: ' + time.strftime("%c"))

    print 'Started grabbing patent grant bibliographic links... ' + time.strftime("%c")
    # Get all patent grant data
    grant_linklist = links_parser("PG", url_source_USPTO)
    print 'Finished grabbing patent grant bibliographic links... ' + time.strftime("%c")
    # Log finished building all zip filepaths
    logger.info('Finished grabbing patent grant bibliographic links: ' + time.strftime("%c"))

    print 'Started grabbing patent application bibliographic links... ' + time.strftime("%c")
    # Get all patent application data
    application_linklist = links_parser("PA", url_source_USPTO)
    print 'Finished grabbing patent application bibliographic links... ' + time.strftime("%c")
    # Log finished building all zip filepaths
    logger.info('Finished grabbing patent application bibliographic links: ' + time.strftime("%c"))

    #print 'Started grabbing patent application pair bibliographic links... ' + time.strftime("%c")
    # Get all patent application pair data
    #application_pair_linklist = links_parser("PAP", url_source_USPTO)
    #print 'Finished grabbing patent application pair bibliographic links... ' + time.strftime("%c")
    # Log finished building all zip filepaths
    #logger.info('Finished grabbing patent application pair bibliographic links: ' + time.strftime("%c"))


    # Return the array of arrays of required links
    return {"grants" : grant_linklist, "applications" : application_linklist, "classifications" : classification_linklist}

# parse HTML file to get links <a>
def links_parser(link_type, url):

    # Define array to hold all links found
    link_array = []
    temp_zip_file_link_array = []
    final_zip_file_link_array = []
    annualized_file_found = False
    annualized_file_link = ""

    # First collect all links on USPTO bulk data page
    content = urllib.urlopen(url).read()
    soup = BeautifulSoup(content, "html.parser")
    for link in soup.find_all('a', href=True):
        # Collet links based on type requested by argument in function call

        # Patent grant
        if link_type == "PG":
            if "https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/" in link['href']:
                link_array.append(link['href'])

        # Patent Application
        elif link_type == "PA":
            if "https://bulkdata.uspto.gov/data/patent/application/redbook/bibliographic/" in link['href']:
                link_array.append(link['href'])

        # Patent Application Pair
        elif link_type == "PAP":
            if "" in link['href']:
                link_array.append(link['href'])

    # Go through each found link on the main USPTO page and get the zip files as links and return that array.
    for item in link_array:
        content = urllib.urlopen(item).read()
        soup = BeautifulSoup(content, "html.parser")
        for link in soup.find_all('a', href=True):
            if ".zip" in link['href']:
                # Check if an annualized link.  If annualized link found then add flag so ONLY that link can be added
                if re.compile("[0-9]{4}.zip").match(link['href']):
                    annualized_file_link = [item + "/" + link['href'], return_file_format_from_filename(link['href'])]
                    annualized_file_found = True
                elif re.compile("[0-9]{4}[0-9_]{1,4}_xml.zip").match(link['href']) is None and re.compile("[0-9]{4}_xml.zip").match(link['href']) is None:
                    temp_zip_file_link_array.append([item + "/" + link['href'], return_file_format_from_filename(link['href'])])

        # Check if Annualized file found
        if annualized_file_found == True:
            if annualized_file_link not in final_zip_file_link_array:
                # Append to the array with format_type
                final_zip_file_link_array.append(annualized_file_link)
        else:
            for link in temp_zip_file_link_array:
                if link not in final_zip_file_link_array:
                    final_zip_file_link_array.append(link)

    print "Number of downloadable .zip files found = " + str(len(final_zip_file_link_array))

    # Return the array links to zip files with absolute urls
    return final_zip_file_link_array
