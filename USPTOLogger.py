# USPTOLogger.py
# USPTO Bulk Data Parser - Processes for Managing Logs
# Description: Processes handles log files.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# Import Python Modules
import logging
import time
import os
import sys

# Setuplogging
def setup_logger(log_file):

    logger = logging.getLogger('USPTO_Database_Construction')
    log_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

# Check the args_array log_lock_file and switch and write file as processed
# TODO accept a passed arg to also write the log as processing, if needed by
# to balance loads using log file in main_process.
def write_process_log(args_array):

    # Set the document type for processing
    document_type = args_array['document_type']

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Print message to stdout and log file
    print "Updating the log for processed file: " + args_array['url_link']
    logger.info("Updating the log for processed file: " + args_array['url_link'])

    # Set the log file to check and rewrite based on the document_type passed
    if document_type == "grant" : log_file_to_rewrite = args_array['grant_process_log_file']
    elif document_type == "application" : log_file_to_rewrite = args_array['application_process_log_file']

    # variable hold while loop running
    log_rewrite_success = 0

    while log_rewrite_success == 0:
        # Create an array to store all lines to be rewritten after
        log_rewrite_array = []
        # Open log_lock_file to check status
        log_lock = open(args_array["log_lock_file"], "r")
        locked_status = log_lock.read().strip()
        log_lock.close()
        #print locked_status

        # If the log lock file is set to open, rewrite log with changes and end while loop
        if locked_status == "0":
            # Write log lock as closed
            log_lock = open(args_array["log_lock_file"], "w")
            log_lock.write("1")
            log_lock.close()
            # Open the appropriate log file
            log_file = open(log_file_to_rewrite, "r")
            # Separate into array of arrays of original csv
            log_file_data_array = log_file.readlines()
            log_file.close()

            # Loop through each line in the file
            for line in log_file_data_array:
                # If the first element in line is the link we have just processed
                line = line.split(",")
                #print line
                if line[0] == args_array["url_link"]:
                    print "Found the URL link in log file"
                    # Append the line with "Processed"
                    log_rewrite_array.append([line[0], line[1], "Processed\n"])
                # If the first element is not the line we are looking for
                else:
                    # Append the line as is
                    log_rewrite_array.append(line)

            # Rewrite the new array to the log file in csv
            log_file = open(log_file_to_rewrite, "w")
            #print log_rewrite_array
            for line in log_rewrite_array:
                #print line[0] + "," + line[1] + "," + line[2]
                log_file.write(line[0] + "," + line[1] + "," + line[2])
            log_file.close()

            # Set the log_lock to open again and close the file.
            log_lock = open(args_array["log_lock_file"], "w")
            log_lock.write("0")
            log_lock.close()

            # Print message to stdout and log file
            print "Log updated for processed file: " + args_array['url_link']
            logger.info("Log updated for processed file: " + args_array['url_link'])

            # End the while loop while by setting file_locked
            log_rewrite_success = 1

        # If the file was found to be locked by another process, close file then wait 1 second
        else:
            #print "waiting on log lock to be opened"
            log_lock.close()
            time.sleep(1)


# Write all log links to files
def write_link_arrays_to_file(all_links_array, args_array):

    # Import Logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Log finished building all zip filepaths
    logger.info('Writing all required links to file ' + time.strftime("%c"))
    # Write all required links into file
    grant_process_file = open(args_array['grant_process_log_file'], "w")
    application_process_file = open(args_array['application_process_log_file'], "w")
    classification_process_file = open(args_array['classification_process_log_file'], "w")
    # Write all grant and application links to separate files
    for item in all_links_array["grants"]:
        grant_process_file.write(item[0] + "," + item[1] + ",Unprocessed\n")
    for item in all_links_array["applications"]:
        application_process_file.write(item[0] + "," + item[1] + ",Unprocessed\n")
    for item in all_links_array["classifications"]:
        classification_process_file.write(item[0] + "," + item[1] + ",Unprocessed\n")
    # Close files
    grant_process_file.close()
    application_process_file.close()
    classification_process_file.close()
    # Log finished building all zip filepaths
    logger.info('Finished writing all .zip filepaths to file ' + time.strftime("%c"))
    # Print message finished writing all links to file
    print "Finished writing all patent grant and application links to files. Finshed Time: " + time.strftime("%c")


# Write all log links to files
def update_link_arrays_to_file(all_links_array, args_array):

    # Import Logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Log finished building all zip filepaths
    logger.info('Updating all required links to file ' + time.strftime("%c"))

    # Open files and read in data to check lines for links that exist already
    grant_process_file = open(args_array['grant_process_log_file'], "r+")
    application_process_file = open(args_array['grant_process_log_file'], "r+")
    grant_process_data_array = grant_process_file.readlines().split(",")
    application_process_data_array = application_process_file.readlines().split(",")

    # Check if new found grant links exist already in file
    for new_item in all_links_array['grants']:
        # Define a flag for if new link found in existing list
        link_found_flag = 0
        # Loop through all existing links found in file
        for item in grant_process_data_array:
            # If match between links is found
            if new_item[0] == item[0]:
                # Set flag that link is found
                link_flag_found == 1
        # If flag is not found
        if link_flag_found == 0:
            # Append the new links to array
            grant_process_data_array.append(new_item[0] + "," + new_item[1] + ",Unprocessed\n")

    # Write the new grant_process_data_array to the original log file
    for item in grant_process_data_array:
        grant_process_file.write(item + "\n")


    # Check if new found grant links exist already in file
    for new_item in all_links_array['applications']:
        # Define a flag for if new link found in existing list
        link_found_flag = 0
        # Loop through all existing links found in file
        for item in application_process_data_array:
            # If match between links is found
            if new_item[0] == item[0]:
                # Set flag that link is found
                link_flag_found == 1
        # If flag is not found
        if link_flag_found == 0:
            # Append the new links to array
            application_process_data_array.append(new_item[0] + "," + new_item[1] + ",Unprocessed\n")

    # Write the new grant_process_data_array to the original log file
    for item in application_process_data_array:
        application_process_file.write(item + "\n")

    # Close files
    grant_process_file.close()
    application_process_file.close()
    # Log finished building all zip filepaths
    logger.info('Finished updating all .zip filepaths to file ' + time.strftime("%c"))
    # Print message finished writing all links to file
    print "Finished updating all patent grant and application links to files. Finshed Time: " + time.strftime("%c")


# Collect all links from file
def collect_all_unstarted_links_from_file(args_array):

    logger = logging.getLogger("USPTO_Database_Construction")

    # Initialize file arrays for temp storage
    grant_temp_array = []
    application_temp_array = []
    classification_temp_array = []

    # Print start message to stdout and log start of reading links from file
    print 'Reading all required links to download and parse ' + time.strftime("%c")
    logger.info('Reading all required links to download and parse ' + time.strftime("%c"))

    try:
        # Read all required grant links into array
        with open(args_array['grant_process_log_file'], "r") as grant_process_file:
            for line in grant_process_file:
                #print line.split(",")[2].replace("\n", "")
                if line.split(",")[2].replace("\n", "") != "Processed":
                    #print "not processed"
                    grant_temp_array.append(line.split(","))

        # Read all required applicaton links into array
        with open(args_array['application_process_log_file'], "r") as application_process_file:
            for line in application_process_file:
                if line.split(",")[2].replace("\n", "") != "Processed":
                    application_temp_array.append(line.split(","))

        # Read all required classification links into array
        with open(args_array['classification_process_log_file'], "r") as classification_process_file:
            for line in classification_process_file:
                if line.split(",")[2].replace("\n", "") != "Processed":
                    classification_temp_array.append(line.split(","))

        # Print finished message to stdout and log file
        print 'Finished reading all required links to download and parse ' + time.strftime("%c")
        logger.info('Finished reading all required links to download and parse ' + time.strftime("%c"))

        # Return the array to main function
        return({"grants" : grant_temp_array, "applications" : application_temp_array, "classifications" : classification_temp_array})

    except Exception as e:
        # Log failure collecting links from file
        print "Failed to get all links from log files " + time.strftime("%c")
        traceback.print_exc()
        logger.error('Failed to get all links from log files ' + time.strftime("%c"))
        # Log exception error messages
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(str(e) + str(exc_type) + str(fname) + str(exc_tb.tb_lineno))
        return False


# Collect all links, or update with new links to log files
def build_or_update_link_files(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Check if links log files exists already
    # If not exists, then find and write all links to file
    #TODO: what if only one log file is missing??  How could that happen??
    if not os.path.isfile(args_array['grant_process_log_file']) or not os.path.isfile(args_array['application_process_log_file']) or not os.path.isfile(args_array['classification_process_log_file']):

        # Print message to stdout and log file
        print "No existing file lists found. Creating them now.  " + time.strftime("%c")
        logger.info('No existing file lists found. Creating them now. ' + time.strftime("%c"))

        try:
            # Get List of all links
            all_links_array = USPTOProcessLinks.get_all_links(args_array)
            #print all_links_array
            write_link_arrays_to_file(all_links_array, args_array)

        except Exception as e:
            # Log failure building all zip filepaths
            print "Failed to get all links from USPTO bulk data site " + time.strftime("%c")
            traceback.print_exc()
            logger.error('Failed to get all links from USPTO bulk data site ' + time.strftime("%c"))
            # Log exception error messages
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error(str(e) + str(exc_type) + str(fname) + str(exc_tb.tb_lineno))

    # Else if the update arg has been passed then update all links files before starting main function
    elif "update" in args_array['command_args']:

        # Print message to stdout and log file
        print "Updating file lists looking for new patent releases.  " + time.strftime("%c")
        logger.info('Updating file lists looking for new patent releases. ' + time.strftime("%c"))

        try:
            # Get List of all links and update the existing links based on found links
            all_links_array = USPTOProcessLinks.get_all_links(args_array)
            update_link_arrays_to_file(all_links_array, args_array)

        except Exception as e:
            # Log finished building all zip filepaths
            print "Failed to get all links from USPTO bulk data site " + time.strftime("%c")
            traceback.print_exc()
            logger.error('Failed to get all links from USPTO bulk data site ' + time.strftime("%c"))
            # Log exception error messages
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error(str(e) + str(exc_type) + str(fname) + str(exc_tb.tb_lineno))
