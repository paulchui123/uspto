# USPTOProcessZipFile.py
# USPTO Bulk Data Parser - Processes ZIP Files
# Description: Imported to Process Modules.  Extracts XML file contents from a downloaded ZIP file.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# ImportPython Modules
import time
import os
import sys
import traceback
import subprocess
import shutil

# Extract a zip file and return the contents of the XML file as an array of lines
def extract_zip_to_array(args_array):
    # Extract the zipfile to read it
    try:
        zip_file = zipfile.ZipFile(args_array['temp_zip_file_name'], 'r')
        # Find the xml file from the extracted filenames
        for filename in zip_file.namelist():
            if '.xml' in filename:
                xml_file_name = filename
        # Print stdout message that xml file was found
        print '[xml file found. Filename: {0}]'.format(xml_file_name)
        logger.info('xml file found. Filename: ' + xml_file_name)
        # Open the file to read lines out of
        xml_file = zip_file.open(xml_file_name, 'r')
        # Extract the contents from the file
        xml_file_contents = xml_file.readlines()
        # Remove the temp files
        urllib.urlcleanup()
        #os.remove(file_name)
        zip_file.close()
        # Return the file contents as array
        return xml_file_contents

    # The zip file has failed using python's ZipFile
    # Unzip the file using subprocess and find the xml file
    except:
        # Print message to stdout
        print '[Zip file ' + args_array['temp_zip_file_name'] + ' failed to unzip with python module]'
        logger.warning('[Zip file ' + args_array['temp_zip_file_name'] + ' failed to unzip with python module]')
        # Pull the filename off the temp zip filename
        temp_base_filename = args_array['temp_zip_file_name'].split('.')[0]
        # Check if an unzip directory exists in the temp directory
        if not os.path.exists(args_array['temp_zip_file_name'] + "/unzip"):
            os.mkdir(args_array['temp_zip_file_name'] + "/unzip")
        # Make a directory for the particular downloaded zip file
        os.mkdir(args_array['temp_zip_file_name'] + "/unzip/" + temp_base_filename)
        # Use a subprocess to unzip linux command
        subprocess.call("unzip " + args_array['temp_zip_file_name'] + " -d " + args_array['temp_zip_file_name'] + "/unzip/" + temp_base_filename, shell=True)
        # Go through each file in the directory and look for the xml file
        for filename in os.listdir(args_array['temp_zip_file_name'] + "/unzip/" + temp_base_filename):
            # Look for file with .xml extention
            if filename.endswith(".xml"):
                xml_file_name = filename
        # Print stdout message that xml file was found
        print '[xml file found. Filename: {0}]'.format(xml_file_name)
        logger.info('xml file found. Filename: ' + xml_file_name)
        # Open the file to read lines out of
        xml_file = open(xml_file_name, 'r')
        # Extract the contents from the file
        xml_file_contents = xml_file.readlines()
        # Remove the temp directory created for the zip contents
        shutil.rmtree(args_array['temp_zip_file_name'] + "/unzip/" + temp_base_filename)
        # Return the file contents as array
        return xml_file_contents

    # Finally, if nothing was returned already, return None
    finally:
        # Print and log that the xml file was not found
        print '[xml file not found.  Filename{0}]'.format(args_array['url_link'])
        logger.error('xml file not found. Filename: ' + args_array['url_link'])
        return None
