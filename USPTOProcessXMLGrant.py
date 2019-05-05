# USPTOProcessXMLGrant.py
# USPTO Bulk Data Parser - Processes XML Grant Files
# Description: Imported to the main USPTOParser.py.  Processes a downloaded grant data files to the extracting.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# ImportPython Modules
import time
import os
import sys
import traceback
import zipfile
import urllib

# Import USPTO Parser Functions
import USPTOLogger
import USPTOSanitizer
import USPTOCSVHandler
import USPTOProcessLinks
import USPTOStoreGrantData


# Function opens the zip file for XML based patent grant files and parses, inserts to database
# and writes log file success
def process_XML_grant_content(args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # If csv file insertion is required, then open all the files
    # into args_array
    if "csv" in args_array['command_args'] or ("database" in args_array['command_args'] and args_array['database_insert_mode'] == "bulk"):
        args_array['csv_file_array'] = USPTOCSVHandler.open_csv_files(args_array['document_type'], args_array['file_name'], args_array['csv_directory'])

    # Process zip file by getting .dat or .txt file and .xml filenames
    start_time = time.time()

    # Extract the zipfile to read it
    zip_file = zipfile.ZipFile(args_array['temp_zip_file_name'],'r')

    # Find the xml file from the extracted filenames
    for name in zip_file.namelist():
        if '.xml' in name:
            xml_file_name = name
            # Print stdout message that xml file was found
            print '[xml file found. Filename: {0}]'.format(xml_file_name)
            logger.info('xml file found. Filename: ' + xml_file_name)

    # Look for the found xml file
    try:
        # Open the file to read lines out of
        xml_file = zip_file.open(xml_file_name, 'r')
    except:
        # Print and log that the xml file was not found
        print '[xml file not found.  Filename{0}]'.format(args_array['url_link'])
        logger.error('xml file not found. Filename: ' + args_array['url_link'])

    # Clean up the zip file
    try:
        # Remove the temp files
        urllib.urlcleanup()
        #os.remove(file_name)
        zip_file.close()
    except:
        # Print and log that xml file could not be closed properly
        print '[Error cleaning up .zip file.  Filename{0}]'.format(args_array['url_link'])
        logger.error('Error cleaning up .zip file. Filename: ' + args_array['url_link'])

    # create variables needed to parse the file
    xml_string = ''
    patent_xml_started = False
    # read through the file and append into groups of string.
    # Send the finished strings to be parsed
    # Use uspto_xml_format to determine file contents and parse accordingly
    #print "The xml format is: " + args_array['uspto_xml_format']
    if args_array['uspto_xml_format'] == "gXML4":

        #print "The xml format is: " + args_array['uspto_xml_format']

        # Loop through all lines in the xml file
        for line in xml_file.readlines():

            # This identifies the start of well formed XML segment for patent
            # grant bibliographic information
            if "<us-patent-grant" in line:
                patent_xml_started = True
                xml_string += "<us-patent-grant>"

            # This identifies end of well-formed XML segement for single patent
            # grant bibliographic information
            elif "</us-patent-grant" in line:

                patent_xml_started = False
                xml_string += line
                # Call the function extract data
                processed_data_array = USPTOProcessLinks.extract_data_router(xml_string, args_array)
                # Call function to write data to csv or database
                USPTOStoreGrantData.store_grant_data(processed_data_array, args_array)

                # reset the xml string
                xml_string = ''

            # This is used to append lines of file when inside single patent grant
            elif patent_xml_started == True:
                # Check which type of encoding should be used to fix the line string
                xml_string += USPTOSanitizer.replace_new_html_characters(line)

    # Used for gXML2 files
    elif args_array['uspto_xml_format'] == "gXML2":

        # Loop through all lines in the xml file
        for line in xml_file.readlines():

            # This identifies the start of well formed XML segment for patent
            # grant bibliographic information
            if "<PATDOC" in line:
                patent_xml_started = True
                xml_string += "<PATDOC>"

                # Print line with number
                #print str(line_number) + " : " + line
                #line_number += 1

            # This identifies end of well-formed XML segement for single patent
            # grant bibliographic information
            elif "</PATDOC" in line:
                patent_xml_started = False
                xml_string += line

                # Call the function extract data
                processed_data_array = USPTOProcessLinks.extract_data_router(xml_string, args_array)
                # Call function to write data to csv or database
                USPTOStoreGrantData.store_grant_data(processed_data_array, args_array)

                # reset the xml string
                xml_string = ''

            # This is used to append lines of file when inside single patent grant
            elif patent_xml_started == True:
                # Check which type of encoding should be used to fix the line string
                xml_string += USPTOSanitizer.replace_old_html_characters(line)

    # Close the .xml file being read from
    xml_file.close()
    # Close all the open .csv files being written to
    USPTOCSVHandler.close_csv_files(args_array)

    # Set a flag file_processed to ensure that the bulk insert succeeds
    file_processed = True

    # If data is to be inserted as bulk csv files, then call the sql function
    if args_array['database_insert_mode'] == 'bulk':
        file_processed = args_array['database_connection'].load_csv_bulk_data(args_array, logger)

    if file_processed:
        # Send the information to USPTOLogger.write_process_log to have log file rewritten to "Processed"
        USPTOLogger.write_process_log(args_array)
        if "csv" not in args_array['command_args']:
            # Delete all the open csv files
            USPTOCSVHandler.delete_csv_files(args_array)

        # Print message to stdout and log
        print '[Loaded {0} data for {1} into database. Time:{2} Finished Time: {3} ]'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c"))
        logger.info('Loaded {0} data for {1} into database. Time:{2} Finished Time: {3}'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c")))

    else:
        # Print message to stdout and log
        print '[Failed to bulk load {0} data for {1} into database. Time:{2} Finished Time: {3} ]'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c"))
        logger.info('[Failed to bulk load {0} data for {1} into database. Time:{2} Finished Time: {3} ]'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c")))

        # TODO: Use the line by line method of the .csv file to load data.
