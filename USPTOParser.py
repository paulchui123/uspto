# USPTO Bulk Data Parser
# Description: Check README.md for instructions on seting up the paser with configuration settings.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# Import Python modules
import xml.etree.ElementTree as ET
import time
import re
import SQLProcessor
import os
import sys
import urllib
import multiprocessing
import logging
from bs4 import BeautifulSoup
import zipfile
import traceback
from HTMLParser import HTMLParser
import csv
#from htmlentitydefs import name2codepoint
import string
import psutil

# Function to accept raw xml data and route to the appropriate function to parse
# either grant, application or PAIR data.
def extract_data_router(xml_data_string, args_array):

    try:
        if args_array['uspto_xml_format'] == "gAPS":
            return extract_APS_grant(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "gXML2":
            return extract_XML2_grant(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "gXML4":
            return extract_XML4_grant(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "aXML1":
            return extract_XML1_application(xml_data_string, args_array)
        elif args_array['uspto_xml_format'] == "aXML4":
            return extract_XML4_application(xml_data_string, args_array)
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

# This function checks if a tag of specified tag_name exists and returns true
# or false if it doesn't exist.
def check_tag_exists(x, tag_name):
    if(x.tag == tag_name): return True
    else: return False

# Function to parse the class and return array of class and subclass
def return_class(class_string):

    # TODO: check the proper way to check parse class string
    # This is problematic but may be handled almost well enough
    # Strings sometimess have spaces but not always, so just cutting the string 0:3 possible takes the first character of the subclass
    mc = class_string[0:3].replace(' ','')
    sc = class_string[3:len(class_string)].replace(" ", "")
    sc1 = sc[0:3].replace(' ','0')
    sc2 = sc[3:len(sc)].replace(' ','')
    if(len(mc) <= 3):
        if(mc.find('D')>-1 and len(mc) == 2):mc = mc[0] + '0' + mc[1]
        elif(len(mc) == 2):mc = '0' + mc
        elif(len(mc) == 1):mc = '00' + mc
    if(len(sc2)<=3):
        if(len(sc2) == 2):sc2 = '0' + sc2
        elif(len(sc2) == 1):sc2 = '00' + sc2
        elif(len(sc2) == 0):sc2 = '000'
    clist = [mc, sc1 + sc2]
    return clist

# Function to parse section, class, and subclass out of XML1 appication data
def return_international_class(class_string):

    # parse the section as first character
    i_class_sec = class_string[0]
    # The Int classification for applications is formatted A00A00/00 so, the first four digits can reliably
    # parsed as the class, and the second group can be parsed as subclass
    i_class = class_string[1:3]
    i_subclass = class_string[3]
    i_class_mgr = class_string[5:7]
    i_class_sgr = class_string[7:len(class_string)].replace("/", "")

    #print i_class_sec
    #print i_class
    #print i_subclass
    #rint i_class_mgr
    #rint i_class_sgr

    # Return array of data
    return [i_class_sec, i_class, i_subclass, i_class_mgr, i_class_sgr]

# returns the cpc class breakdown by class and subclass
def return_cpc_class(class_string):
    cpc_class_sec = class_string[0]
    class_string = class_string[1:len(class_string)]



# Function to accept the date and return in MYSQL formated date
# TODO: fix the date parsing. Problem of possibly one or two characters for month and day
def return_formatted_date(time_str, args_array, document_id):

    logger = logging.getLogger("USPTO_Database_Construction")

    # Check if None has been passed in
    if time_str is None:
        return None
        logger.warning("None Type object was found as date for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])

    # Check if '0000-01-01' has been passed in
    elif time_str == '0000-01-01':
        return None
        logger.warning("'0000-01-01' was found as date for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
    else:
        if len(time_str) ==  8:
            if  time_str[4:6] == "00" : month = "01"
            else: month = time_str[4:6]
            if time_str[6:8] == "00" : day = "01"
            else: day = time_str[6:8]
            return time_str[0:4] + '-' + month + '-' + day
        elif len(time_str) == 9:
            #print "Date length == 9"
            time_str = time_str.replace("\n", "").replace("\r", "")
            if len(time_str) == 9:
                # Log that a bad date was found and could not be cleaned
                logger.warning("Malformed date was found on length == 9 string: " + time_str + " for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
            if  time_str[4:6] == "00" : month = "01"
            else: month = time_str[4:6]
            if time_str[6:8] == "00" : day = "01"
            else: day = time_str[6:8]
            return time_str[0:4] + '-' + month + '-' + day
        else:
            # Log that a bad date was found and could not be cleaned
            logger.warning("Malformed date was found on length != 8 or 9 string: " + time_str + " for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
            return None

# ***** used to fix patent numbers *****
def return_patent_number(patternStr,inputStr):
    c = re.compile(patternStr)
    r = c.match(inputStr)
    return r

# strip leading zero and '&' from patent numbers
def fix_patent_number(document_id):

    # Strip the leading zeros from the number if exists
    if str(document_id[0]) ==  '0':
        document_id = document_id[1:len(str(document_id))]

    # Strip the `&` from the document_id
    document_id = document_id.replace("&", "")
    # Strip the `*` from document_id
    document_id = document_id.replace("*", "")
    # Strip lowercase `e` from patent number
    document_id = document_id.replace("e", "E")

    # TODO: strip trailing dash and number from document_id

    # return the fixed patent number
    return document_id

# Strip leading zeros from ....
def strip_leading_zeros(string):
    return string.lstrip("0")

# strips tags from XMLTree element
def return_element_text(xmlElement):
    if(ET.iselement(xmlElement)):
        elementStr = ET.tostring(xmlElement)
        # Strip tags, whitespace and newline and carriage returns
        element_text =  re.sub('<[^<]*>', '', elementStr)
        # If string is empty, then return None, else encode adn return as UTF-8 and escape characters
        if element_text == "":
            return None
        else: return element_text

    else:return None

# Escapes old html values for xml
def escape_value_for_sql(value):
    # TODO: maybe need to change.  This is hoping to strip all unicode and then encode to utf-8
    value = value.replace("'", "\'")
    value = value.replace('"', '\'')
    value = value.replace('\n', '')
    value = value.replace('\r', '')
    return value

# APS classes need to be separated into class and subclass
def fix_old_APS_class(class_string):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # If the length is 6, then class is first three digits, subclass is next 3
    if len(class_string) == 6:
        return [class_string[0:3], class_string[3:6]]
    # If the length is 5, then check for desin patent and last 3 are the subclass
    elif len(class_string) == 5:
        if class_string[0] == "D":
            return [class_string[0:2], class_string[2:5], "MAL"]
        # TODO: check that the class is parsed correctly.  How to do this??
        else:
            # Assume the first three are class and remainder are subclass
            return [class_string[0:3], class_string[3:5], "MAL"]

    # Else print a message and log, return first three as class and rest as subclass for now
    # TODO: sub classes with `.` missing for sub-sub class and sub-class length is not parsed.
    # TODO: is this correct?
    else:
        #print "Strange Class String - malformed length: " + class_string
        #logger.warning("Strange Class String - malformed length: " + class_string)
        return [class_string[0:3], class_string[3:len(str(class_string))], "MAL"]

# Function used to extract data from XML4 formatted patent grants
def extract_XML4_grant(raw_data, args_array):

    logger = logging.getLogger("USPTO_Database_Construction")

    url_link = args_array['url_link']
    uspto_xml_format = args_array['uspto_xml_format']

    # Define all arrays to hold the data
    processed_grant = []
    processed_applicant = []
    processed_examiner = []
    processed_assignee = []
    processed_agent = []
    processed_inventor = []
    processed_usclass = []
    processed_intclass = []
    processed_cpcclass = []
    processed_gracit = []
    processed_forpatcit = []
    processed_nonpatcit = []

    # Stat process timer
    start_time = time.time()

    # Pass the raw_data data into Element Tree
    patent_root = ET.fromstring(raw_data)

    # Start the extraction of XML data
    for r in patent_root.findall('us-bibliographic-data-grant'):

        # Find the main patent grant data
        for pr in r.findall('publication-reference'):
            for di in pr.findall('document-id'):
                try: pub_country = di.findtext('country')
                except: pub_country = None
                try:
                    document_id = di.findtext('doc-number')
                    document_id = fix_patent_number(document_id)
                except:
                    document_id = None
                    logger.error("No Patent Number was found for: " + url_link)
                try: kind = di.findtext('kind')
                except: kind = None
                try: pub_date = return_formatted_date(di.findtext('date'), args_array, document_id)
                except: pub_date = None

        # Find the main application data
        for ar in r.findall('application-reference'):
            try: app_type = ar.attrib['appl-type']
            except: app_type = None
            for di in ar.findall('document-id'):
                try: app_country = di.findtext('country')
                except: app_country = None
                try: app_no = di.findtext('doc-number')
                except: app_no = None
                try: app_date = return_formatted_date(di.findtext('date'), args_array, document_id)
                except: app_date = None

        # Get the series code
        try: series_code = r.findtext('us-application-series-code')
        except: series_code = None

        # Get the length of grant
        try: terms_of_grant = r.find("us-term-of-grant").findtext("length-of-grant")
        except: terms_of_grant = None

        # Find all international classifications
        ic = r.find('classifications-ipcr')
        position = 1
        if ic is not None:
            for icc in ic.findall('classification-ipcr'):
                for x in icc.getchildren():
                    if(check_tag_exists(x,'section')) :
                        try: i_class_sec = x.text
                        except: i_class_sec = None
                    if(check_tag_exists(x,'class')) :
                        try: i_class_cls = x.text
                        except:  i_class_cls = None
                    if(check_tag_exists(x,'subclass')) :
                        try: i_class_sub = x.text
                        except: i_class_sub = None
                    if(check_tag_exists(x,'main-group')) :
                        try: i_class_mgr = x.text
                        except: i_class_mgr = None
                    if(check_tag_exists(x,'subgroup')) :
                        try: i_class_sgr = x.text
                        except: i_class_sgr = None

                # Append SQL data into dictionary to be written later
                processed_intclass.append({
                    "table_name" : "uspto.INTCLASS_G",
                    "GrantID" : document_id,
                    "Position" : position,
                    "Section" : i_class_sec,
                    "Class" : i_class_cls,
                    "SubClass" : i_class_sub,
                    "MainGroup" : i_class_mgr,
                    "SubGroup" : i_class_sgr,
                    "FileName" : args_array['file_name']
                })

                position += 1

        # Find all CPC classifications
        cpc = r.find('us-field-of-classification-search')
        #print nat_class_element
        if cpc is not None:
            position = 1
            for cpcc in cpc.findall('classification-cpc-text'):

                try:
                    #print cpc.text
                    cpc_text = cpcc.text
                    #print cpc_text
                    cpc_class_string, cpc_group_string = cpc_text.split(" ")
                    #print cpc_class_string + " " + cpc_group_string
                    cpc_class_sec = cpc_text[0]
                    cpc_class = cpc_class_string[1:3]
                    cpc_subclass = cpc_class_string[3]
                    cpc_class_mgr, cpc_class_sgr = cpc_group_string.rsplit("/", 1)
                    #print cpc_class_sec + " " + cpc_class + " " + cpc_subclass + " " + cpc_class_mgr + " " + cpc_class_sgr
                except:
                    #traceback.print_exc()
                    cpc_class_sec = None
                    cpc_class = None
                    cpc_subclass = None
                    cpc_class_mgr = None
                    cpc_class_sgr = None
                    logger.warning("There was an error parsing the cpc class for Grant ID: " + document_id + " in file: " + url_link)
                    logger.warning("Traceback: " + traceback.format_exc())

                # Append SQL data into dictionary to be written later
                processed_cpcclass.append({
                    "table_name" : "uspto.CPCCLASS_G",
                    "GrantID" : document_id,
                    "Position" : position,
                    "Section" : cpc_class_sec,
                    "Class" : cpc_class,
                    "SubClass" : cpc_subclass,
                    "MainGroup" : cpc_class_mgr,
                    "SubGroup" : cpc_class_sgr,
                    "FileName" : args_array['file_name']
                })

                position += 1

        # Find all US classifications
        for nc in r.findall('classification-national'):
            position = 1
            try:
                n_class_info = nc.findtext('main-classification')
                n_class_main, n_subclass = return_class(n_class_info)
            except:
                n_class_main = None
                n_subclass = None

            # Append SQL data into dictionary to be written later
            processed_usclass.append({
                "table_name" : "uspto.USCLASS_G",
                "GrantID" : document_id,
                "Position" : position,
                "Class" : n_class_main,
                "SubClass" : n_subclass,
                "FileName" : args_array['file_name']
            })

            position += 1

            n_class_fur_root = nc.findall('further-classification') #return a list of all elements
            for n in n_class_fur_root:
                try: n_class_info = n.text
                except: n_class_info = None
                try: n_class_main, n_subclass = return_class(n_class_info)
                except:
                    n_class_main = None
                    n_subclass = None

                # Append SQL data into dictionary to be written later
                processed_usclass.append({
                    "table_name" : "uspto.USCLASS_G",
                    "GrantID" : document_id,
                    "Position" : position,
                    "Class" : n_class_main,
                    "SubClass" : n_subclass,
                    "FileName" : args_array['file_name']
                })

                position += 1

        # Find the title of the patent
        try: title = r.findtext('invention-title')
        except: title = None

        # Find all references cited in the grant
        for rf in r.findall('us-references-cited'):
            for rfc in rf.findall('us-citation'):
                # If the patent citation child is found must be a patent citation
                if(rfc.find('patcit') != None):
                    position = 1
                    try: citation_position = strip_leading_zeros(rfc.find('patcit').attrib['num'])
                    except: citation_position = position
                    for x in rfc.findall('patcit'):
                        try: citation_country = x.find('document-id').findtext('country')
                        except: citation_country = None
                        try: citation_grant_id = x.find('document-id').findtext('doc-number')
                        except: citation_grant_id = None
                        try: citation_kind = x.find('document-id').findtext('kind')
                        except: citation_kind = None
                        try: citation_name = x.find('document-id').findtext('name')
                        except: citation_name = None
                        try: citation_date = x.find('document-id').findtext('date')
                        except: citation_date = None
                        try:
                            if rfc.findtext('category') == "cited by examiner":
                                citation_category = 1
                            else:
                                citation_category = 0
                        except: citation_category = None

                    # US patent citations
                    if(citation_country.strip().upper() == 'US'):

                        # Append SQL data into dictionary to be written later
                        processed_gracit.append({
                            "table_name" : "uspto.GRACIT_G",
                            "GrantID" : document_id,
                            "Position" : citation_position,
                            "CitedID" : citation_grant_id,
                            "Kind" : citation_kind,
                            "Name" : citation_name,
                            "Date" : return_formatted_date(citation_date, args_array, document_id),
                            "Country" : citation_country,
                            "Category" : citation_category,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

                    elif(citation_country.strip().upper() != 'US'):

                        # Append SQL data into dictionary to be written later
                        processed_forpatcit.append({
                            "table_name" : "uspto.FORPATCIT_G",
                            "GrantID" : document_id,
                            "Position" : citation_position,
                            "CitedID" : citation_grant_id,
                            "Kind" : citation_kind,
                            "Name" : citation_name,
                            "Date" : return_formatted_date(citation_date, args_array, document_id),
                            "Country" : citation_country,
                            "Category" : citation_category,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

                # If the non patent citations are found
                elif(rfc.find('nplcit') != None):
                    position = 1
                    for x in rfc.findall('nplcit'):
                        try: citation_position = strip_leading_zeros(rfc.find('nplcit').attrib['num'])
                        except: citation_position = position
                        # Sometimes, there will be '<i> or <sup>, etc.' in the reference string; we need to remove it
                        try: non_patent_citation_text = x.findtext('othercit')
                        except: non_patent_citation_text = None
                        # TODO: check that strip tags is working
                        try: non_patent_citation_text = re.sub('<[^>]+>','',non_patent_citation_text).replace('\n', "")
                        except: non_patent_citation_text = None
                        # TODO: parse the category into boolean for now  How many categories are there and what are they??
                        # TODO: change category to boolean in schema
                        try:
                            if x.findtext('category') == "cited by examiner":
                                citation_category = 1
                            else:
                                citation_category = 0
                        except:
                            citation_category = None

                        # Append SQL data into dictionary to be written later
                        processed_nonpatcit.append({
                            "table_name" : "uspto.NONPATCIT_G",
                            "GrantID" : document_id,
                            "Position" : citation_position,
                            "Citation" : non_patent_citation_text,
                            "Category" : citation_category,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

        # Find number of claims
        try: claims_num = r.findtext('number-of-claims')
        except: claims_num = None

        # Find the number of figures and number of drawings
        nof = r.find('figures')
        try: number_of_drawings = nof.findtext('number-of-drawing-sheets')
        except: number_of_drawings = None
        try: number_of_figures = nof.findtext('number-of-figures')
        except: number_of_figures = None

        # Find the parties
        for prt in r.findall('us-parties'):
            # Find all applicant data
            for apts in prt.findall('us-applicants'):
                position = 1
                for apt in apts.findall('us-applicant'):
                    # TODO: strip leading zeros fromm sequence number
                    try: applicant_sequence = strip_leading_zeros(apt.attrib['sequence'])
                    except: applicant_sequence = position
                    if(apt.find('addressbook') != None):
                        try: applicant_orgname = apt.find('addressbook').findtext('orgname')
                        except: applicant_orgname = None
                        try: applicant_first_name = apt.find('addressbook').findtext('first-name')
                        except: applicant_first_name = None
                        try: applicant_last_name = apt.find('addressbook').findtext('last-name')
                        except: applicant_last_name = None
                        try: applicant_city = apt.find('addressbook').find('address').findtext('city')
                        except: applicant_city = None
                        try: applicant_state = apt.find('addressbook').find('address').findtext('state')
                        except: applicant_state = None
                        try: applicant_country = apt.find('addressbook').find('address').findtext('country')
                        except: applicant_country = None

                        # Append SQL data into dictionary to be written later

                        processed_applicant.append({
                            "table_name" : "uspto.APPLICANT_G",
                            "GrantID" : document_id,
                            "OrgName" : applicant_orgname,
                            "Position" : applicant_sequence,
                            "FirstName" : applicant_first_name,
                            "LastName" : applicant_last_name,
                            "City" : applicant_city,
                            "State" : applicant_state,
                            "Country" : applicant_country,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

            # Find all inventor data
            for apts in prt.findall('inventors'):
                position = 1
                for apt in apts.findall('inventor'):
                    try: inventor_sequence = strip_leading_zeros(apt.attrib['sequence'])
                    except: inventor_sequence = position
                    if(apt.find('addressbook') != None):
                        try: inventor_first_name = apt.find('addressbook').findtext('first-name')
                        except: inventor_first_name = None
                        try: inventor_last_name = apt.find('addressbook').findtext('last-name')
                        except: inventor_last_name = None
                        try: inventor_city = apt.find('addressbook').find('address').findtext('city')
                        except: inventor_city = None
                        try: inventor_state = apt.find('addressbook').find('address').findtext('state')
                        except: inventor_state = None
                        try: inventor_country = apt.find('addressbook').find('address').findtext('country')
                        except: inventor_country = None
                        try: inventor_residence = apt.find('addressbook').find('address').findtext('country')
                        except: inventor_residence = None

                        # Append SQL data into dictionary to be written later

                        processed_inventor.append({
                            "table_name" : "uspto.INVENTOR_G",
                            "GrantID" : document_id,
                            "Position" : inventor_sequence,
                            "FirstName" : inventor_first_name,
                            "LastName" : inventor_last_name,
                            "City" : inventor_city,
                            "State" : inventor_state,
                            "Country" : inventor_country,
                            "Residence" : inventor_residence,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

            # Find all agent data
            for agns in prt.findall('agents'):
                position = 1
                for agn in agns.findall('agent'):
                    try: agent_sequence = strip_leading_zeros(agn.attrib['sequence'])
                    except: agent_sequence = position
                    if(agn.find('addressbook') != None):
                        try: agent_orgname = agn.find('addressbook').findtext('orgname')
                        except: agent_orgname = None
                        try: agent_last_name = agn.find('addressbook').findtext('last-name')
                        except: agent_last_name = None
                        try: agent_first_name = agn.find('addressbook').findtext('first-name')
                        except: agent_first_name = None
                        try: agent_country = agn.find('addressbook').find('address').findtext('country')
                        except: agent_country = None

                        # Append SQL data into dictionary to be written later
                        processed_agent.append({
                            "table_name" : "uspto.AGENT_G",
                            "GrantID" : document_id,
                            "Position" : agent_sequence,
                            "OrgName" : agent_orgname,
                            "LastName" : agent_last_name,
                            "FirstName" : agent_first_name,
                            "Country" : agent_country,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

        # Find all assignee data
        for asn in r.findall('assignees'):
            position = 1
            for x in asn.findall('assignee'):
                if(x.find('addressbook') != None):
                    try: asn_orgname = x.find('addressbook').findtext('orgname')
                    except: asn_orgname = None
                    try: asn_role = x.find('addressbook').findtext('role')
                    except: asn_role = None
                    try: asn_city = x.find('addressbook').find('address').findtext('city')
                    except: asn_city = None
                    try: asn_state = x.find('addressbook').find('address').findtext('state')
                    except: asn_state = None
                    try: asn_country = x.find('addressbook').find('address').findtext('country')
                    except: asn_country = None

                    # Append SQL data into dictionary to be written later
                    processed_assignee.append({
                        "table_name" : "uspto.ASSIGNEE_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : asn_orgname,
                        "Role" : asn_role,
                        "City" : asn_city,
                        "State" : asn_state,
                        "Country" : asn_country,
                        "FileName" : args_array['file_name']
                    })

                    position += 1

        # Find all examiner data
        for exm in r.findall('examiners'):
            for x in exm.findall('primary-examiner'):
                try: exm_last_name = x.findtext('last-name')
                except: exm_last_name = None
                try: exm_first_name = x.findtext('first-name')
                except: exm_first_name = None
                try: exm_department = x.findtext('department')
                except: exm_department = None

                # Append SQL data into dictionary to be written later
                processed_examiner.append({
                    "table_name" : "uspto.EXAMINER_G",
                    "GrantID" : document_id,
                    "Position" : 1,
                    "LastName" : exm_last_name,
                    "FirstName" : exm_first_name,
                    "Department" : exm_department,
                    "FileName" : args_array['file_name']
                })

            for x in exm.findall('assistant-examiner'):
                try: exm_last_name = x.findtext('last-name')
                except: exm_last_name = None
                try: exm_first_name = x.findtext('first-name')
                except: exm_first_name = None
                try: exm_department = x.findtext('department')
                except: exm_department = None

                # Append SQL data into dictionary to be written later
                processed_examiner.append({
                    "table_name" : "uspto.EXAMINER_G",
                    "GrantID" : document_id,
                    "Position" : 2,
                    "LastName" : exm_last_name,
                    "FirstName" : exm_first_name,
                    "Department" : exm_department,
                    "FileName" : args_array['file_name']
                })

    # TODO: see if it's claims or description and store accordingly
    try: claims = patent_root.findtext('description')
    except: claims = None
    #print claims

    # Find the abstract
    try:
        abstract = return_element_text(patent_root.find('abstract'))
    except:
        traceback.print_exc()
        abstract = None
    #print abstract

    # Append SQL data into dictionary to be written later
    try:
        processed_grant.append({
            "table_name" : "uspto.GRANT",
            "GrantID" : document_id,
            "Title" : title,
            "IssueDate" : pub_date,
            "Kind" : kind,
            "USSeriesCode" : series_code,
            "Abstract" : abstract,
            "ClaimsNum" : claims_num,
            "DrawingsNum" : number_of_drawings,
            "FiguresNum" : number_of_figures,
            "ApplicationID" : app_no,
            "Claims" : claims,
            "FileDate" : app_date,
            "AppType" : app_type,
            "GrantLength" : terms_of_grant,
            "FileName" : args_array['file_name']
        })
    except Exception as e:
        print "could not append to array"
        traceback.print_exc()
        logger.warning("Could not append patent data to array for patent number: " + document_id + " Traceback: " + traceback.format_exc())

    # Return a dictionary of the processed_ data arrays
    return {
        "processed_grant" : processed_grant,
        "processed_applicant" : processed_applicant,
        "processed_examiner" : processed_examiner,
        "processed_assignee" : processed_assignee,
        "processed_agent" : processed_agent,
        "processed_inventor" : processed_inventor,
        "processed_usclass" : processed_usclass,
        "processed_intclass" : processed_intclass,
        "processed_cpcclass" : processed_cpcclass,
        "processed_gracit" : processed_gracit,
        "processed_forpatcit" : processed_forpatcit,
        "processed_nonpatcit" : processed_nonpatcit
    }

# Function used to extract data from XML2 formatted patent grants
def extract_XML2_grant(raw_data, args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    url_link = args_array['url_link']
    uspto_xml_format = args_array['uspto_xml_format']

    #print raw_data

    # Define all arrays needed to hold the data
    processed_grant = []
    processed_applicant = []
    processed_examiner = []
    processed_assignee = []
    processed_agent = []
    processed_inventor = []
    processed_usclass = []
    processed_intclass = []
    processed_gracit = []
    processed_forpatcit = []
    processed_nonpatcit = []

    # Start timer
    start_time = time.time()

    try:
        # Pass the raw data into Element tree xml object
        patent_root = ET.fromstring(raw_data)

    except ET.ParseError as e:
        print_xml = raw_data.split("\n")
        for num, line in enumerate(print_xml, start = 1):
            print str(num) + ' : ' + line
        logger.error("Character Entity prevented ET from parsing XML")
        # Print traceback
        traceback.print_exc()
        # Print exception information to file
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

    # Start the parsing process for XML
    for r in patent_root.findall('SDOBI'): #us-bibliographic-data-grant'):

        # Collect document data
        for B100 in r.findall('B100'): #GRANT
            try:
                document_id = return_element_text(B100.find('B110')) # PATENT GRAND NUMBER
                document_id = fix_patent_number(document_id)
            except:
                document_id = None
                logger.error("No Patent Number was found for: " + url_link)
            try: kind = return_element_text(B100.find('B130'))
            except: kind = None
            try: pub_date = return_element_text(B100.find('B140')) # PATENT ISSUE DATE
            except: pub_date = None
            try: pub_country = return_element_text(B100.find('B190')) # PATENT APPLICANT COUNTRY??
            except: pub_country = None

        # Collect apllication data in document
        for B200 in r.findall('B200'): # APPLICATION
            # TODO: find these datas in XML2 applications
            app_type = None
            app_country = None
            try: app_no = return_element_text(B200.find('B210')) # APPLICATION NUMBER
            except: app_no = None
            try: app_date = return_element_text(B200.find('B220')) # APPLICATION DATE
            except: app_date = None
            try: series_code = return_element_text(B200.find('B211US'))
            except: series_code = None

        # Collect the grant length
        grant_length = return_element_text(r.find("B474"))

        # Collect US classification
        for B500 in r.findall('B500'):
            for B520 in B500.findall('B520'): #US CLASSIFICATION
                position = 1
                for B521 in B520.findall('B521'): # USCLASS MAIN
                    n_class_info = return_element_text(B521)
                    n_class_main, n_subclass = return_class(n_class_info)

                    # Append SQL data into dictionary to be written later
                    processed_usclass.append({
                        "table_name" : "uspto.USCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "Class" : n_class_main,
                        "SubClass" : n_subclass,
                        "FileName" : args_array['file_name']
                    })

                    position += 1
                for B522 in B520.findall('B522'): # USCLASS FURTHER
                    n_class_info = return_element_text(B522)
                    n_class_main, n_subclass = return_class(n_class_info)

                    # Append SQL data into dictionary to be written later
                    processed_usclass.append({
                        "table_name" : "uspto.USCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "Class" : n_class_main,
                        "SubClass" : n_subclass,
                        "FileName" : args_array['file_name']
                    })

                    position += 1

            # Collect International Class data
            # TODO: check if I need to set all variables to empty or can just leave as null
            # TODO: check if classification is parsed correctly
            for B510 in B500.findall('B510'): # INTERNATIONAL CLASS
                #logger.warning("International Classifcation found in XML2: " + args_array['url_link'] + " document: " + str(document_id))
                # Reset position
                position = 1
                for B511 in B510.findall('B511'): #MAIN CLASS
                    i_class_version_date = None
                    i_class_action_date = None
                    i_class_gnr = None
                    i_class_level = None
                    i_class_sec = None
                    int_class = return_element_text(B511)
                    # TODO: check international classification and rewrite this parsing piece.
                    if(len(int_class.split())>1):
                        i_class, i_subclass = int_class.split()
                    else:
                        i_class = int_class
                        i_subclass = None
                    i_class_mgr = None
                    i_class_sgr = None
                    i_class_sps = None
                    i_class_val = None
                    i_class_status = None
                    i_class_ds = None

                    # Append SQL data into dictionary to be written later
                    processed_intclass.append({
                        "table_name" : "uspto.INTCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "Section" : i_class_sec,
                        "Class" : i_class,
                        "SubClass" : i_subclass,
                        "MainGroup" : i_class_mgr,
                        "SubGroup" : i_class_sgr,
                        "FileName" : args_array['file_name']
                    })

                    position += 1

                for B512 in B510.findall('B511'): #INTERNATIONAL CLASS FURTHER
                    i_class_version_date = None
                    i_class_action_date = None
                    i_class_gnr = None
                    i_class_level = None
                    i_class_sec = None
                    int_class = return_element_text(B512)
                    # TODO: splitting int class does not include possible multiple subclasses
                    if(len(int_class.split())>1):
                        i_class = int_class.split()[0]
                        i_subclass = int_class.split()[1]
                    else:
                        i_class = int_class
                        i_subclass = None
                    i_class_mgr = None
                    i_class_sgr = None
                    i_class_sps = None
                    i_class_val = None
                    i_class_status = None
                    i_class_ds = None

                    # Append SQL data into dictionary to be written later
                    processed_intclass.append({
                        "table_name" : "uspto.INTCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "Section" : i_class_sec,
                        "Class" : i_class,
                        "SubClass" : i_subclass,
                        "MainGroup" : i_class_mgr,
                        "SubGroup" : i_class_sgr,
                        "FileName" : args_array['file_name']
                    })

                    position += 1

            # Collect Tite
            for B540 in B500.findall('B540'):
                try: title = return_element_text(B540) #TITLE
                except: title = None

            # Collect Citations
            for B560 in B500.findall('B560'): # CITATIONS

                # Reset position counter for all citations loop
                position = 1

                for B561 in B560.findall('B561'): #PATCIT
                    # TODO: find out how to do PCIT, DOC without loop.  Only B561 needs loop
                    PCIT = B561.find('PCIT')
                    # Determien if the patent is US or not
                    #TODO: needs to check better, what does non US patent look like
                    # If all patents have PARTY-US then perhaps a databse call to check the country of origin
                    # would still allow to separate into GRACIT and FORPATCIT_G
                    #if PCIT.find("PARTY-US") == True:
                        #print "CiTATION OUNTRY US"
                        #citation_country == "US"
                    #else:
                        #citation_country = "NON-US"
                        #logger.warning("NON US patent found")

                    citation_country = "US"

                    DOC = PCIT.find('DOC')
                    try: citation_document_number = return_element_text(DOC.find('DNUM'))
                    except: citation_document_number = None
                    try: pct_kind = return_element_text(DOC.find('KIND'))
                    except: pct_kind = None
                    try: citation_date = return_formatted_date(return_element_text(DOC.find('DATE'), args_array, document_id))
                    except: citation_date = None
                    try: citation_name = return_element_text(PCIT.find('PARTY-US'))
                    except: citation_name = None

                    # Parse citation category
                    if(len(B561.getchildren()) > 1):
                        citation_category = B561.getchildren()[1].tag.replace("\n", "").replace("\r", "")
                        #print type(citation_category)
                        # TODO: check that the citation category tag matches correctly
                        #print "Citation Category = " + citation_category + " Length: " + str(len(citation_category))
                        if "CITED-BY-EXAMINER" in citation_category:
                            citation_category = 1
                        elif "CITED-BY-OTHER" in citation_category:
                            citation_category = 2
                        else:
                            citation_category = 0
                            logger.warning("Cited by unknown type")
                    else: citation_category = None

                    #TODO: be aware that there may be something crazy in the citation document number
                    if citation_country == "US":

                        # Append SQL data into dictionary to be written later
                        processed_gracit.append({
                            "table_name" : "uspto.GRACIT_G",
                            "GrantID" : document_id,
                            "Position" : position,
                            "CitedID" : citation_document_number,
                            "Kind" : pct_kind,
                            "Name" : citation_name,
                            "Date" : citation_date,
                            "Country" : citation_country,
                            "Category" : citation_category,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

                    else:

                        # Append SQL data into dictionary to be written later
                        processed_forpatcit.append({
                            "table_name" : "uspto.FORPATCIT_G",
                            "GrantID" : document_id,
                            "Position" : position,
                            "CitedID" : citation_document_number,
                            "Kind" : pct_kind,
                            "Name" : citation_name,
                            "Date" : citation_date,
                            "Country" : citation_country,
                            "Category" : citation_category,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

                # Reset position counter for non-patent citations loop
                position = 1
                for B562 in B560.findall('B562'): #NON-PATENT LITERATURE
                    for NCIT in B562.findall('NCIT'):
                        # sometimes, there will be '<i> or <sup>, etc.' in the reference string; we need to remove it
                        non_patent_citation_text = return_element_text(NCIT)
                        non_patent_citation_text = re.sub('<[^>]+>','',non_patent_citation_text)

                        # parse citation cateory into code
                        ncitation_category = ET.tostring(NCIT)
                        if(len(B562.getchildren())>1):
                            ncitation_category = B562.getchildren()[1].tag.replace("\n", "").replace("\r", "")
                            #print type(ncitation_category)
                            #rint "Non patent citation category" + ncitation_category
                        if "CITED-BY-EXAMINER" in ncitation_category:
                            ncitation_category = 1
                        elif "CITED-BY-OTHER" in ncitation_category:
                            ncitation_category = 2
                        else:
                            ncitation_category = 0


                    # Append SQL data into dictionary to be written later
                    processed_nonpatcit.append({
                        "table_name" : "uspto.NONPATCIT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "Citation" : non_patent_citation_text,
                        "Category" : ncitation_category,
                        "FileName" : args_array['file_name']
                    })

                    position += 1

            # Collect number of claims
            for B570 in B500.findall('B570'):
                try: claims_num = return_element_text(B570.find('B577'))
                except: claims_num = None

            # Collect number of drawings and figures
            for B590 in B500.findall('B590'):
                for B595 in B590.findall('B595'):
                    try: number_of_drawings = return_element_text(B595)
                    except: number_of_drawings = None
                for B596 in B590.findall('B596'):
                    try: number_of_figures = return_element_text(B596)
                    except: number_of_figures = None

            # TODO: B582 find out what it is.  Looks like patent classifications but it's all alone in the XML

        # Collect party information
        # TODO: find the applicant data and append to array
        for B700 in r.findall('B700'): #PARTIES

            # Collect inventor data
            for B720 in B700.findall('B720'): #INVENTOR
                # Reset position for inventors
                position = 1

                # Collect inventor information
                for B721 in B720.findall('B721'):
                    for i in B721.findall('PARTY-US'):
                        itSequence = position
                        try: inventor_first_name = return_element_text(i.find('NAM').find('FNM'))
                        except: inventor_first_name = None
                        try: inventor_last_name = return_element_text(i.find('NAM').find('SNM'))
                        except: inventor_last_name = None
                        try: inventor_city = return_element_text(i.find('ADR').find('CITY'))
                        except: inventor_city = None
                        try: inventor_state = return_element_text(i.find('ADR').find('STATE'))
                        except: inventor_state = None
                        # TODO: find out if country can be other than US
                        inventor_country = "US"
                        inventor_nationality = None
                        inventor_residence = None

                    # Append SQL data into dictionary to be written later
                    processed_inventor.append({
                        "table_name" : "uspto.INVENTOR_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "FirstName" : inventor_first_name,
                        "LastName" : inventor_last_name,
                        "City" : inventor_city,
                        "State" : inventor_state,
                        "Country" : inventor_country,
                        "Nationality" : inventor_nationality,
                        "Residence" : inventor_residence,
                        "FileName" : args_array['file_name']
                    })

                    position += 1

            # Collect Assignee data
            # TODO: check if finding child of child is working
            # Reset position for assignees
            position = 1
            for B730 in B700.findall('B730'): #ASSIGNEE
                for B731 in B730.findall('B731'):
                    for x in B731.findall('PARTY-US'):
                        try: asn_orgname = return_element_text(x.find('NAM').find("ONM"))
                        except: asn_orgname = None
                        asn_role = None
                        try: asn_city = return_element_text(x.find("ADR").find('CITY'))
                        except: asn_city = None
                        try: asn_state = return_element_text(x.find("ADR").find('STATE'))
                        except: asn_state = None
                        # TODO: find out if country is always US because it's never included.  Check all other references also
                        asn_country = "US"

                    # Append SQL data into dictionary to be written later
                    processed_assignee.append({
                        "table_name" : "uspto.ASSIGNEE_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : asn_orgname,
                        "Role" : asn_role,
                        "City" : asn_city,
                        "State" :  asn_state,
                        "Country" : asn_country,
                        "FileName" : args_array['file_name']
                    })

                    # Increment the position placement
                    position += 1

            # Collect agent data
            for B740 in B700.findall('B740'): #AGENT
                # Reset position for agents
                position = 1
                for B741 in B740.findall('B741'):
                    for x in B741.findall('PARTY-US'):
                        try: agent_orgname = return_element_text(x.find('NAM').find("ONM"))
                        except: agent_orgname = None
                        try: agent_last_name = return_element_text(i.find('NAM').find('FNM'))
                        except: agent_last_name = None
                        try: agent_first_name = return_element_text(i.find('NAM').find('SNM'))
                        except: agent_first_name = None
                        agent_country = "US"

                        # Append SQL data into dictionary to be written later
                        processed_agent.append({
                            "table_name" : "uspto.AGENT_G",
                            "GrantID" : document_id,
                            "Position" : position,
                            "OrgName" : agent_orgname,
                            "LastName" : agent_last_name,
                            "FirstName" : agent_first_name,
                            "Country" : agent_country,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

            # Collect examiner data
            for B745 in B700.findall('B745'): #PERSON ACTING UPON THE DOC
                position = 1
                for B746 in B745.findall('B746'): #PRIMARY EXAMINER
                    for x in B746.findall('PARTY-US'):
                        try: examiner_last_name = return_element_text(x.find('NAM').find('SNM'))
                        except: examiner_last_name = None
                        try: examiner_fist_name = return_element_text(x.find('NAM').find('FNM'))
                        except:  examiner_fist_name = None
                        #TODO: find out if 748US is the department
                        examiner_department = None

                        # Append SQL data into dictionary to be written later
                        processed_examiner.append({
                            "table_name" : "uspto.EXAMINER_G",
                            "GrantID" : document_id,
                            "Position" : position,
                            "LastName" :  examiner_last_name,
                            "FirstName" : examiner_fist_name,
                            "Department" : examiner_department,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

                for B747 in B745.findall('B747'): #ASSISTANT EXAMINER
                    for x in B747.findall('PARTY-US'):
                        try: examiner_last_name = return_element_text(x.find('NAM').find('SNM'))
                        except: examiner_last_name = None
                        try: examiner_fist_name = return_element_text(x.find('NAM').find('FNM'))
                        except: examiner_fist_name = None
                        #TODO: find out if 748US is the department
                        examiner_department = None

                        # Append SQL data into dictionary to be written later
                        processed_examiner.append({
                            "table_name" : "uspto.EXAMINER_G",
                            "GrantID" : document_id,
                            "Position" : position,
                            "LastName" :  examiner_last_name,
                            "FirstName" : examiner_fist_name,
                            "Department" : examiner_department,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

        # Collect Abstract from data
        try:
            abstr = patent_root.find('SDOAB')
            abstract = return_element_text(abstr)
            #print abstract
        except: abstract = None

        # Collect claims from data
        try:
            cl = patent_root.find('SDOCL')
            claims = return_element_text(cl)
            #print claims
        except:
            traceback.print_exc()
            claims = None


        # Append SQL data into dictionary to be written later
        processed_grant.append({
            "table_name" : "uspto.GRANT",
            "GrantID" : document_id,
            "Title" : title,
            "IssueDate" : pub_date,
            "Kind" : kind,
            "GrantLength" : grant_length,
            "USSeriesCode" : series_code,
            "Abstract" : abstract,
            "ClaimsNum" : claims_num,
            "DrawingsNum" : number_of_drawings,
            "FiguresNum" : number_of_figures,
            "ApplicationID" : app_no,
            "Claims" : claims,
            "FileDate" : app_date,
            "AppType" : app_type,
            "FileName" : args_array['file_name']
        })


    # Return a dictionary of the processed_ data arrays
    return {
        "processed_grant" : processed_grant,
        "processed_applicant" : processed_applicant,
        "processed_examiner" : processed_examiner,
        "processed_assignee" : processed_assignee,
        "processed_agent" : processed_agent,
        "processed_inventor" : processed_inventor,
        "processed_usclass" : processed_usclass,
        "processed_intclass" : processed_intclass,
        "processed_gracit" : processed_gracit,
        "processed_forpatcit" : processed_forpatcit,
        "processed_nonpatcit" : processed_nonpatcit
    }


# Used to parse xml files of the type APS
def process_APS_grant_content(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # If csv file insertion is required, then open all the files
    # into args_array
    if "csv" in args_array['command_args'] or ("database" in args_array['command_args'] and args_array['database_insert_mode'] == "bulk"):
        args_array['csv_file_array'] = open_csv_files(args_array['document_type'], args_array['file_name'], args_array['csv_directory'])

    # Colect arguments from args array
    url_link = args_array['url_link']
    uspto_xml_format = args_array['uspto_xml_format']

    # Define all arrays to hold the data
    processed_grant = []
    processed_applicant = []
    processed_examiner = []
    processed_assignee = []
    processed_agent = []
    processed_inventor = []
    processed_usclass = []
    processed_intclass = []
    processed_gracit = []
    processed_forpatcit = []
    processed_nonpatcit = []

    # Process zip file by getting .dat or .txt file and .xml filenames
    start_time = time.time()

    # Extract the zipfile to read it
    zip_file = zipfile.ZipFile(args_array['temp_zip_file_name'],'r')

    data_file_name = ""
    for name in zip_file.namelist():
        if '.dat' in name or '.txt' in name:
            data_file_name = name

    # If xml file not found, then print error message
    if data_file_name == "":
        # Print and log that the xml file was not found
        print '[APS .dat data file not found.  Filename{0}]'.format(args_array['url_link'])
        logger.error('APS .dat file not found. Filename: ' + args_array['url_link'])

    # Process zip file contents of .dat or .txt file and .xml files
    data_reader = zip_file.open(data_file_name,'r')
    # Remove the temp files
    urllib.urlcleanup()
    #os.remove(file_name)
    zip_file.close()

    # Define variables required to parse the file
    patent_started = False
    next_line_loaded_already = False
    end_of_file = False


    # Start to read the file in lines
    while end_of_file == False:

        # read a single line if there is no line content then load another line
        if next_line_loaded_already == False:
            # Load the next line
            line = data_reader.readline()

        # Every time through the loop, initialize that next line is not loaded
        next_line_loaded_already = False

        # Strip whitespace from line
        line = line.strip()
        #print line
        #print len(line)

        # If return value is EOF
        if not line:

            #print "End of file found"
            # Set flag to end the while loop
            end_of_file = True

            # Store the final patent file for the data
            # Define variables that are not included in APS format
            kind = None
            app_type = None
            # Check if variable exists for abstract and claims and create if not set already
            if 'abstract' not in locals(): abstract = None
            if 'claims' not in locals(): claims = None
            if 'grant_length' not in locals(): grant_length = None

            # Append to the patent grand data
            processed_grant.append({
                "table_name" : "uspto.GRANT",
                "GrantID" : document_id,
                "Title" :  title,
                "IssueDate" : pub_date,
                "FileDate" : app_date,
                "Kind" : kind,
                "AppType": app_type,
                "USSeriesCode" : series_code,
                "Abstract" : abstract,
                "ClaimsNum" : claims_num,
                "DrawingsNum" : number_of_drawings,
                "FiguresNum" : number_of_figures,
                "ApplicationID" : app_no,
                "GrantLength" : grant_length,
                "FileName" : args_array['file_name']
            })

            # Append to the processed data array
            processed_data_array = {
                "processed_grant" : processed_grant,
                "processed_applicant" : processed_applicant,
                "processed_examiner" : processed_examiner,
                "processed_assignee" : processed_assignee,
                "processed_agent" : processed_agent,
                "processed_inventor" : processed_inventor,
                "processed_usclass" : processed_usclass,
                "processed_intclass" : processed_intclass,
                "processed_gracit" : processed_gracit,
                "processed_forpatcit" : processed_forpatcit,
                "processed_nonpatcit" : processed_nonpatcit
            }

            # Call function to write data to csv or database
            #print "Starting the data storage process of " + args_array['uspto_xml_format'] + " data for contents of: " + args_array['url_link'] + " Started at: " + time.strftime("%c")
            store_grant_data(processed_data_array, args_array)

            # Update the processed file line here

        # If the line is start of a patent document
        elif line[0:4] == "PATN":

            if patent_started == True:

                # Define variables that are not included in APS format
                kind = None
                app_type = None
                # Check if variable exists for abstract and claims and create if not set already
                if 'abstract' not in locals(): abstract = None
                if 'claims' not in locals(): claims = None
                if 'grant_length' not in locals(): grant_length = None

                # Append to the patent grand data
                processed_grant.append({
                    "table_name" : "uspto.GRANT",
                    "GrantID" : document_id,
                    "Title" :  title,
                    "IssueDate" : pub_date,
                    "FileDate" : app_date,
                    "Kind" : kind,
                    "AppType": app_type,
                    "USSeriesCode" : series_code,
                    "Abstract" : abstract,
                    "ClaimsNum" : claims_num,
                    "DrawingsNum" : number_of_drawings,
                    "FiguresNum" : number_of_figures,
                    "ApplicationID" : app_no,
                    "GrantLength" : grant_length,
                    "FileName" : args_array['file_name']
                })

                # Reset all variables required to store data to avoid overlap
                document_id = None
                title = None
                pub_date = None
                app_date = None
                kind = None
                app_type = None
                series_code = None
                abstract = None
                claims_num = None
                number_of_drawings = None
                number_of_figures = None
                app_no = None
                grant_length = None

                #print processed_grant

                # Append to the processed data array
                processed_data_array = {
                    "processed_grant" : processed_grant,
                    "processed_applicant" : processed_applicant,
                    "processed_examiner" : processed_examiner,
                    "processed_assignee" : processed_assignee,
                    "processed_agent" : processed_agent,
                    "processed_inventor" : processed_inventor,
                    "processed_usclass" : processed_usclass,
                    "processed_intclass" : processed_intclass,
                    "processed_gracit" : processed_gracit,
                    "processed_forpatcit" : processed_forpatcit,
                    "processed_nonpatcit" : processed_nonpatcit
                }

                #print processed_usclass

                # Reset all arrays to hold the data
                processed_grant = []
                processed_applicant = []
                processed_examiner = []
                processed_assignee = []
                processed_agent = []
                processed_inventor = []
                processed_usclass = []
                processed_intclass = []
                processed_gracit = []
                processed_forpatcit = []
                processed_nonpatcit = []

                # Call function to write data to csv or database
                #print "Starting the data storage process of " + args_array['uspto_xml_format'] + " data for contents of: " + args_array['url_link'] + " Started at: " + time.strftime("%c")
                store_grant_data(processed_data_array, args_array)

            # If first line found that starts a patent set flag to true
            else:
                # Set the patent started to true
                patent_started = True

        # New patent line was not found, expect other data elements to be found.
        # Parse elements by loading new lines until section parsed.
        # Check for a header.  If found, then append header.

        # WKU is patent number
        elif line[0:4].strip() == "WKU":
            document_id = None
            try:
                # TODO: need to filter patent numbers in function ???
                document_id = fix_patent_number(replace_old_html_characters(line[3:].strip()))
            except:
                # TODO: exception should be logged since patent number is required.
                document_id = None
                logger.error("No patent number found for patent from this url: " + args_array["url_link"])

        # Series Code
        elif line[0:4].strip() == "SRC":
            try: series_code = replace_old_html_characters(line[3:].strip())
            except: series_code = None
        # Number of Claims
        elif line[0:4].strip() == "NCL":
            try: claims_num = replace_old_html_characters(line[3:].strip()).split(",")[0]
            except: claims_num = None
        # ISD is Publication Date
        elif line[0:4].strip() == "ISD":
            try: pub_date = return_formatted_date(replace_old_html_characters(line[3:].strip()), args_array, document_id)
            except: pub_date = None
        # APN is application number
        elif line[0:4].strip() == "APN":
            try: app_no = fix_patent_number(replace_old_html_characters(line[3:].strip()))
            except: app_no = None
        # APD is Application date
        elif line[0:4].strip() == "APD":
            try: app_date = return_formatted_date(replace_old_html_characters(line[3:].strip()), args_array, document_id)
            except: app_date = None
        # TTL is title
        elif line[0:4].strip() == "TTL":
            # Grab the text from the line of TTL
            try: title = replace_old_html_characters(line[3:].strip())
            except: title = None

            # If TTL found, can be multi-line.  Load next line and check if should be appended or not
            line = data_reader.readline()
            #print line

            # Check if line has empty header
            if not line[0:3].strip():
                # Append the TTL to the title variable above if empty header
                title += replace_old_html_characters(line[3:].strip())
            # Check if the loaded next line is another type of data
            elif line[0:4].strip() == "ISD":
                # Set that the next line has been loaded already so it can be found
                next_line_loaded_already = True
                line = line.strip()

        # Number of Drawings
        elif line[0:4].strip() == "NDR":
            try: number_of_drawings = replace_old_html_characters(line[3:].strip()).split(",")[0].replace(" ", "")
            except: number_of_drawings = None
        # Number of Figures
        elif line[0:4].strip() == "NFG":
            try: number_of_figures = replace_old_html_characters(line[3:].strip()).split(",")[0].replace(" ", "")
            except: number_of_drawings = None
        # Term length of patent
        elif line[0:4].strip() == "TRM":
            try: grant_length = replace_old_html_characters(line[3:].strip()).split(",")[0].replace(" ", "")
            except: grant_length = None
        # Assistant Examiner
        elif line[0:4].strip() == "EXA":
            try:
                assistant_examiner = replace_old_html_characters(line[3:].strip()).split(";")
                examiner_last_name = assistant_examiner[0]
                examiner_first_name = assistant_examiner[1]
            except:
                examiner_first_name = None
                examiner_last_name = None

            # Append SQL data into dictionary to be written later
            processed_examiner.append({
                "table_name" : "uspto.EXAMINER_G",
                "GrantID" : document_id,
                "Position" : 2,
                "LastName" : examiner_last_name,
                "FirstName" : examiner_first_name,
                "Department" : None,
                "FileName" : args_array['file_name']
            })

            # Reset all the variables to avoid overlap
            examiner_last_name = None
            examiner_first_name = None

            #print processed_examiner

        # Primary Examiner
        elif line[0:4].strip() == "EXP":
            examiner_fist_name = None
            examiner_last_name = None
            try:
                primary_examiner = replace_old_html_characters(line[3:].strip()).split(";")
                examiner_last_name = primary_examiner[0]
                examiner_first_name = primary_examiner[1]
            except:
                examiner_last_name = None
                examiner_first_name = None

            # Append SQL data into dictionary to be written later
            processed_examiner.append({
                "table_name" : "uspto.EXAMINER_G",
                "GrantID" : document_id,
                "Position" : 1,
                "LastName" : examiner_last_name,
                "FirstName" : examiner_first_name,
                "Department" : None,
                "FileName" : args_array['file_name']
            })

            # Reset all variables to avoid overlap
            examiner_first_name = None
            examiner_last_name = None

            #print processed_examiner

        # Foreign Reference
        elif line[0:4] == "UREF":
            # This header type  has no data on same line but will include further
            # readlines so read another line in a while loop until you finish with foreign references
            # and when non-foreign nonreference is found set a flag that prevents another line from being
            # read next iteration through main loop

            # Set required variables and arrays
            accepted_headers_array = ["UREF", "OCL", "PNO", "ISD", "NAM", "OCL", "XCL", "UCL"]
            position = 1
            data_parse_completed = False

            while data_parse_completed == False:
                # Read next line
                line = data_reader.readline().strip()

                # If line is represents another foreign reference, store the last one into array
                if line[0:4] == "UREF":

                    # The data collection is complete and should be appended
                    if item_ready_to_insert == True:

                        # Try to append the item.  If items are missingn it will not append
                        # and error will be written to log
                        try:
                            # Append SQL data into dictionary to be written later
                            processed_gracit.append({
                                "table_name" : "uspto.GRACIT_G",
                                "GrantID" : document_id,
                                "Position" : position,
                                "CitedID" : citation_document_number,
                                "Name" : citation_name,
                                "Date" : citation_date,
                                "Country" : "US",
                                "FileName" : args_array['file_name']
                            })

                            # Reset all variables to avoid overlap
                            citation_document_number = None
                            citation_name = None
                            citation_date = None

                            #print processed_gracit

                            # Increment position for next possible foreign patent reference
                            position += 1
                            # Reset the item ready to insert
                            item_ready_to_insert = False

                        except Exception as e:
                            # Reset the item ready to insert
                            item_ready_to_insert = False
                            print "Data missing from patent references for grant id : " + document_id + " in url: " + args_array['url_links']
                            logger.error("Some data was missing from the patent reference data for grant id: " + document_id + " in url: " + args_array['url_link'])

                # CitedID
                elif line[0:3] == "PNO":
                    try: citation_document_number = fix_patent_number(replace_old_html_characters(line[3:].strip().replace("*", "").replace(" ", "")))
                    except: citation_document_number = None
                # Issue Date of cited patent
                elif line[0:3] == "ISD":
                    try: citation_date = return_formatted_date(replace_old_html_characters(line[3:].strip()), args_array, document_id)
                    except: citation_date = None
                # Name of patentee
                elif line[0:3] == "NAM":
                    try:
                        citation_name = replace_old_html_characters(line[3:].strip())
                        item_ready_to_insert = True
                    except:
                        citation_name = None
                        item_ready_to_insert = True

                # Catch the tag of next header but not empty line
                elif line[0:4].strip() not in accepted_headers_array:
                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # Break the foreign patent citation loop
                    data_parse_completed = True

        # Other References
        elif line[0:4] == "OREF":

            accepted_headers_array = ["PAL"]
            # Initialize the position
            position = 1
            # Initialize empty string to to hold multi-line entries
            temp_data_string = ''
            # End while loop
            data_parse_completed = False

            # Loop through all OREF until finished
            while data_parse_completed == False:

                # Read next line
                line = data_reader.readline()

                # If line is represents another foreign reference, store the last one into array
                if line[0:3] == "PAL":

                    # If the temp_data_string is not empty then append that record
                    if temp_data_string:

                        # Append SQL data into dictionary to be written later
                        processed_nonpatcit.append({
                            "table_name" : "uspto.NONPATCIT_G",
                            "GrantID" : document_id,
                            "Position" : position,
                            "Citation" : temp_data_string,
                            "Category" : None,
                            "FileName" : args_array['file_name']
                        })

                        #print processed_nonpatcit

                        # Reset variable to avoid overlap
                        temp_data_string = None

                        # Increment counter position
                        position += 1
                        # Set the temp_data_string back to empty
                        temp_data_string = ''

                    # The PAL header was found and temp_data_string is empty.
                    # Try to collect the PAL data
                    else:

                        # Get the citation text from the PAL line
                        try: temp_data_string += replace_old_html_characters(line[3:].strip())
                        except:
                            logger.error("A non patent reference could not be found for grant_id: " + document_id + " in link: " + args_array['url_link'])
                            temp_data_string = ''

                # Catch the tag of next header but not empty line
                elif not line[0].strip():

                    try:
                        # Append the continued reference text to temp string
                        temp_data_string += replace_old_html_characters(line[3:].strip())
                    except Exception as e:
                        logger.error("A non patent reference could not be appended for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # If the next element in the document is found
                elif line[0:4].strip() not in accepted_headers_array:

                    # Complete a final append to the other referenes array
                    # Append SQL data into dictionary to be written later
                    processed_nonpatcit.append({
                        "table_name" : "uspto.NONPATCIT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "Citation" : temp_data_string,
                        "Category" : None,
                        "FileName" : args_array['file_name']
                    })

                    # Reset variable to avoid overlap
                    temp_data_string = None

                    #print processed_nonpatcit

                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # End while loop
                    data_parse_completed = True

        # Foreign Reference
        elif line[0:4] == "FREF":
            # This header type  has no data on same line but will include further
            # readlines so read another line in a while loop until you finish with foreign references
            # and when non-foreign nonreference is found set a flag that prevents another line from being
            # read next iteration through main loop
            accepted_headers_array = ["FREF", "PNO", "ISD", "CNT", "ICL", "OCL"]
            # Init position
            position = 1
            # Init while loop break
            data_parse_completed = False

            while data_parse_completed == False:
                # Read next line
                line = data_reader.readline().strip()
                #print "FREF"

                # If line is represents another foreign reference, store the last one into array
                if line[0:4] == "FREF":

                    # The data collection is complete and should be appended
                    if item_ready_to_insert == True:

                        # Try to append the item.  If items are missingn it will not append
                        # and error will be written to log
                        try:
                            # Append SQL data into dictionary to be written later
                            processed_forpatcit.append({
                                "table_name" : "uspto.FORPATCIT_G",
                                "GrantID" : document_id,
                                "Position" : position,
                                "CitedID" : citation_document_number,
                                "Date" : (citation_date, args_array),
                                "Country" : citation_country,
                                "FileName" : args_array['file_name']
                            })

                            # Reset variable to avoid overlap
                            citation_document_number = None
                            citation_date = None
                            citation_country = None

                            #print processed_forpatcit

                            # Increment position for next possible foreign patent reference
                            position += 1
                            # Reset the item ready to insert
                            item_ready_to_insert = False

                        except Exception as e:
                            print "Data missing from foreign references for grant id : " + document_id + " in url: " + args_array['url_links']
                            logger.error("Some data was missing from the Foreign reference data for grant id: " + document_id + " in url: " + args_array['url_link'])
                            # Reset the item ready to insert
                            item_ready_to_insert = False

                elif line[0:3] == "PNO":
                    citation_document_number = None
                    try: citation_document_number = replace_old_html_characters(line[3:].strip())
                    except: citation_document_number = None
                elif line[0:3] == "ISD":
                    citation_date = None
                    try: citation_date = replace_old_html_characters(line[3:].strip())
                    except: citation_date = None
                elif line[0:3] == "CNT":
                    citation_country = None
                    # Country is the last item included in APS so item is ready to be inserted after this is found
                    try:
                        citation_country = replace_old_html_characters(line[3:].strip())
                        item_ready_to_insert = True
                    except:
                        citation_country = None
                        item_ready_to_insert = True

                # If the tag found is not for FREF data, new data set found.
                elif line[0:4].strip() not in accepted_headers_array:

                    # Append SQL data into dictionary to be written later
                    processed_forpatcit.append({
                        "table_name" : "uspto.FORPATCIT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "CitedID" : citation_document_number,
                        "Date" : return_formatted_date(citation_date, args_array, document_id),
                        "Country" : citation_country,
                        "FileName" : args_array['file_name']
                    })

                    # Reset variable to avoid overlap
                    citation_document_number = None
                    citation_date = None
                    citation_country = None

                    #print processed_forpatcit

                    # Increment position for next possible foreign patent reference
                    position += 1

                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # Break the foreign patent citation loop
                    data_parse_completed = True

        # Classification Data
        elif line[0:4] == "CLAS":

            # Define accepted array headers for this section
            accepted_headers_array = ["OCL", "XCL", "UCL", "DCL", "EDF", "ICL", "FSC", "FSS"]
            # Set the variable that will certainly not be found
            i_class_mgr = None
            i_class_sgr = None
            # Initialize the position variables required
            position_uclass = 1
            position_intclass = 1
            # Initialize empty string to to hold multi-line entries
            temp_data_string = ''
            # Set flag for while loop
            data_parse_completed = False
            # Set the malformmed_class variable used to find erroneos classes
            malformed_class = 0

            # Loop through all OREF until finished
            while data_parse_completed == False:

                # Read next line
                line = data_reader.readline().strip()

                # Collect the main class
                if line[0:3] == "OCL":

                    # Get the citation text from the line
                    try:
                        class_string = replace_old_html_characters(line[3:].strip().replace("  ", " ")).split(" ")
                        #print "Original class_string array: "
                        #print class_string
                        #print "Class string found in patent: " + document_id + " class:" + str(class_string[0]) + " substring: " + str(class_string[1])
                        #logger.warning("Class string found in patent: " + document_id + " class:" + str(class_string[0]) + " substring: " + str(class_string[1]))
                        # If the class string's length is 6, then assumed that needs to be parsed further.
                        if len(class_string) == 1:
                            if len(str(class_string[0])) >= 4 and len(str(class_string[0])) <= 12 :
                                class_string = fix_old_APS_class(str(class_string[0]))

                                # Set the returned array to insert data vaiables
                                n_class_main = class_string[0]
                                n_subclass = class_string[1]

                                # if the class is malformed than set variable to = 1
                                if "MAL" in class_string:
                                    malformed_class = 1

                        elif len(class_string) == 2:
                            if len(class_string[0]) > 3:
                                n_class_main = class_string[0][0:3]
                                n_subclass = class_string[0][3:len(class_string)] + class_string[1]
                            else:
                                n_class_main = class_string[0]
                                n_subclass = class_string[1]

                        elif len(class_string) == 3:
                            n_class_main = class_string[0]
                            n_subclass = class_string[1] + " " + class_string[2]
                            malformed_class = 1
                        else:
                            logger.warning("A mal-formed US OCL class was found with more than one space: " + document_id + " in link: " + args_array['url_link'])

                    except Exception as e:
                        # Print exception information to file
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        n_class_main = None
                        n_subclass = None
                        logger.error("An OCL classification error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # Append SQL data into dictionary to be written later
                    processed_usclass.append({
                        "table_name" : "uspto.USCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position_uclass,
                        "Class" : n_class_main,
                        "SubClass" : n_subclass,
                        "Malformed" : malformed_class,
                        "FileName" : args_array['file_name']
                    })

                    # Reset the class and subclass
                    class_string = None
                    n_class_main = None
                    n_subclass = None
                    malformed_class = 0

                    #print processed_usclass

                    # Increment position for US class
                    position_uclass += 1

                # Collect the main class
                if line[0:3] == "XCL":

                    # Get the US class text from the line
                    try:
                        class_string = replace_old_html_characters(line[3:].strip().replace("  ", " ")).split(" ")
                        #print "Class string found in patent: " + document_id + " class:" + str(class_string[0]) + " substring: " + str(class_string[1])
                        #logger.warning("Class string found in patent: " + document_id + " class:" + str(class_string[0]) + " substring: " + str(class_string[1]))
                        # If the class string's length is 6, then assumed that needs to be parsed further.

                        if len(class_string) == 1:
                            if len(str(class_string[0])) >= 4 and len(str(class_string[0])) <= 12 :
                                class_string = fix_old_APS_class(str(class_string[0]))

                                # Set the returned array to insert data vaiables
                                n_class_main = class_string[0]
                                n_subclass = class_string[1]

                                # if the class is malformed than set variable to = 1
                                if "MAL" in class_string:
                                    malformed_class = 1

                        elif len(class_string) == 2:
                            if len(class_string[0]) > 3:
                                n_class_main = class_string[0][0:3]
                                n_subclass = class_string[0][3:len(class_string)] + class_string[1]
                            else:
                                n_class_main = class_string[0]
                                n_subclass = class_string[1]
                        elif len(class_string) == 3:
                            n_class_main = class_string[0]
                            n_subclass = class_string[1] + " " + class_string[2]
                            malformed_class = 1
                        else:
                            logger.warning("A mal-formed US XCL class was found with more than two spaces: " + document_id + " in link: " + args_array['url_link'])

                        # if the class is malformed than set variable to = 1
                        if "MAL" in class_string:
                            malformed_class = 1

                    except:
                        # Print exception information to file
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        n_class_main = None
                        n_subclass = None
                        logger.error("An XCL classification error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # Append SQL data into dictionary to be written later
                    processed_usclass.append({
                        "table_name" : "uspto.USCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position_uclass,
                        "Class" : n_class_main,
                        "SubClass" : n_subclass,
                        "Malformed" : malformed_class,
                        "FileName" : args_array['file_name']
                    })

                    # Reset the class and subclass
                    class_string = None
                    n_class_main = None
                    n_subclass = None
                    malformed_class = 0

                    #print processed_usclass

                    # Increment position for US class
                    position_uclass += 1

                # Collect the main class
                elif line[0:3] == "ICL":

                    # Get the international class text from the line
                    # TODO: find out how to parse the int class code.
                    try:
                        i_class_string = replace_old_html_characters(line[3:].strip().replace("  ", " ")).split(" ")
                    except:
                        # Print exception information to file
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        i_class_main = None
                        i_subclass = None
                        logger.error("An International classification error occurred that could not be extracted for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    try:
                        if len(i_class_string) == 1:
                            i_class_main = i_class_string[0]
                            i_subclass = None
                            malformed_class = 1
                        elif len(i_class_string) == 2:
                            if len(i_class_string[0]) > 3:
                                i_class_main = i_class_string[0]
                                i_subclass = i_class_string[0][3:len(i_class_string)] + i_class_string[1]
                            else:
                                i_class_main = i_class_string[0]
                                i_subclass = i_class_string[1]
                        elif len(i_class_string) == 3:
                            n_class_main = i_class_string[0]
                            n_subclass = i_class_string[1] + " " + i_class_string[2]
                            malformed_class = 1
                        else:
                            logger.warning("A mal-formed international class was found (" + i_class_string + ") with more than two spaces: " + document_id + " in link: " + args_array['url_link'])
                    except:
                        # Print exception information to file
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        i_class_main = None
                        i_subclass = None
                        logger.error("An International classification error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])


                    # TODO: find out if field of search is same as Main Group, etc.
                    # Append SQL data into dictionary to be written later
                    processed_intclass.append({
                        "table_name" : "uspto.INTCLASS_G",
                        "GrantID" : document_id,
                        "Position" : position_intclass,
                        "Class" : i_class_main,
                        "SubClass" : i_subclass,
                        "MainGroup" : i_class_mgr,
                        "SubGroup" : i_class_sgr,
                        "Malformed" : malformed_class,
                        "FileName" : args_array['file_name']
                    })

                    # Reset the class and subclass
                    i_class_string = None
                    i_class_main = None
                    i_subclass = None
                    malformed_class = 0

                    #print processed_intclass

                    # Increment International class
                    position_intclass += 1


                # Looking for next line in reference, id'd by not empty temp_data_string and not empty line
                elif line.strip() and temp_data_string != '':

                    try:
                        # Append the continued reference text to temp string
                        temp_data_string += replace_old_html_characters(line[3:].strip())
                    except Exception as e:
                        logger.error("A international class reference could not be appended for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # If the next element in the document is found
                elif line[0:4].strip() not in accepted_headers_array:

                    # End while loop
                    data_parse_completed = True
                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # Break the foreign patent citation loop


        # Abstract
        elif line[0:4] == "ABST":

            # Define accepted headers
            accepted_headers_array = ["PAL", "PAR"]
            # Initialize empty string to to hold multi-line entries
            abstract = ''
            # Set the flag for while loop
            data_parse_completed = False

            # Loop through all ABST until finished
            while data_parse_completed == False:

                line = data_reader.readline()
                # Read next line if not then set end_of_file = True
                if not line:
                    data_parse_completed = True
                    end_of_file = True
                # The file was read so file continues
                else:

                    # If line is represents another foreign reference, store the last one into array
                    if line[0:3] == "PAL" or line[0:3] == "PAR":

                        # Get the citation text from the line
                        try:
                            abstract += replace_old_html_characters(line[3:].strip())
                        except:
                            logger.error("A abstract reference could not be found for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # If line has blank space at first character
                    elif not line[0].strip():
                        # Used to catch when the last line of the file is found during a abstract parse
                        try:
                            # Append the continued reference text to temp string
                            abstract += replace_old_html_characters(line[3:].strip())
                        except:
                            traceback.print_exc()
                            logger.error("A abstract reference append error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


                    # If the next element in the document is found
                    elif line[0:4].strip() not in accepted_headers_array:

                        # Set claims to None is still empty string
                        if not abstract:
                            abstract = None

                        # Set the next_line_loaded_already flag to True
                        next_line_loaded_already = True
                        # Break the foreign patent citation loop
                        data_parse_completed = True


        # Claims
        elif line[0:4] == "DCLM":

            accepted_headers_array = ["PAL"]
            # Initialize empty string to to hold multi-line entries
            claims = ''
            temp_claims_string = ''
            data_parse_completed = False

            # Loop through all CLAIMS until finished
            while data_parse_completed == False:

                # Read next line
                line = data_reader.readline()

                # If line is represents another foreign reference, store the last one into array
                if line[0:3] == "PAL":

                    # Get the citation text from the line
                    try:
                        claims = replace_old_html_characters(line[3:].strip())
                    except:
                        logger.error("A claim reference could not be found for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # If line is not empty then append to claims string
                elif not line[0].strip():

                    try:
                        # Append the continued reference text to temp string
                        claims += replace_old_html_characters(line[3:].strip())
                    except Exception as e:
                        traceback.print_exc()
                        logger.error("A claim append reference could not be found for grant_id: " + document_id + " in link: " + args_array['url_link'])
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

                # If the next element in the document is found
                elif line[0:4].strip() not in accepted_headers_array:

                    # Set claims to None is still empty string
                    if not claims:
                        claims = None

                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # Break the foreign patent citation loop
                    data_parse_completed = True

        # Inventor
        elif line[0:4] == "INVT":

            # Init the position
            position = 1
            # Set the multi_line_flag to empty
            multi_line_flag = ""
            # Array of expected header strings
            accepted_headers_array = ["INVT", "NAM","STR", "CTY", "STA", "CNT", "ZIP", "R47", "ITX"]
            # Init ready to insert to false to first flag is not caught
            item_ready_to_insert = False
            # Set loop flag for finished reading inventors to false
            data_parse_completed = False
            # Ensure that all variables that are optionally included will be set.
            inventory_first_name = None
            inventor_last_name = None
            inventor_city = None
            inventor_state = None
            inventor_country = None
            inventor_residence = None
            inventor_nationality = None

            # loop through all inventors
            while data_parse_completed == False:

                # Read next line
                line = data_reader.readline()
                #print line

                # If the inventor tag is found then append last set of data
                if line[0:4] == "INVT":

                    # The data collection is complete and should be appended
                    if item_ready_to_insert == True:

                        # Append SQL data into dictionary to be written later
                        try:
                            processed_inventor.append({
                                "table_name" : "uspto.INVENTOR_G",
                                "GrantID" : document_id,
                                "Position" : position,
                                "FirstName" : inventor_first_name,
                                "LastName" : inventor_last_name,
                                "City" : inventor_city,
                                "State" : inventor_state,
                                "Country" : inventor_country,
                                "Nationality" : inventor_nationality,
                                "Residence" : inventor_residence,
                                "FileName" : args_array['file_name']
                            })

                            #print processed_inventor

                            # Reset all the variables associated so they don't get reused
                            inventor_first_name = None
                            inventor_last_name = None
                            inventor_city = None
                            inventor_nationality = None
                            inventor_country = None
                            inventor_residence = None
                            inventor_state = None

                            # Increment the position
                            position += 1
                            # Reset the item ready to insert
                            item_ready_to_insert = False

                        except Exception as e:
                            print "Data missing from inventors for grant id : " + document_id + " in url: " + args_array['url_links']
                            logger.error("Some data was missing from inventors reference data for grant id: " + document_id + " in url: " + args_array['url_link'])
                            # Reset the item ready to insert
                            item_ready_to_insert = False


                # Get and pase the name of the inventor
                elif line[0:3] == "NAM":
                    # Get the citation text from the line
                    try:
                        name_string = replace_old_html_characters(line[3:].strip()).split(";")
                        if len(name_string) == 1:
                            inventor_last_name = name_string[0].strip()
                            inventor_first_name = None
                        elif len(name_string) == 2:
                            inventor_first_name = name_string[0].strip()
                            inventor_last_name = name_string[1].strip()
                        else:
                            inventor_first_name = name_string[0].strip()
                            inventor_last_name = name_string[1].strip()

                    except:
                        traceback.print_exc()
                        # Print exception information to file
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        inventor_first_name = None
                        inventor_last_name = None
                        logger.error("A inventor name data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the street of inventor
                elif line[0:3] == "STR":
                    multi_line_flag = 'STR'
                    # Get the citation text from the line
                    try:
                        inventor_residence = replace_old_html_characters(line[3:].strip())
                    except:
                        inventor_residence = None
                        logger.error("A inventor street data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the street of inventor
                elif line[0:3] == "CTY":
                    # Get the citation text from the line
                    try:
                        inventor_city = replace_old_html_characters(line[3:].strip())
                    except:
                        inventor_city = None
                        logger.error("A inventor city data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the state of inventor
                elif line[0:3] == "STA":
                    # Get the citation text from the line
                    try:
                        inventor_state = replace_old_html_characters(line[3:].strip())
                    except:
                        inventor_state = None
                        logger.error("A inventor state data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # get the country of inventor
                elif line[0:3] == "CNT":
                    # Get the citation text from the line
                    try:
                        inventor_country = replace_old_html_characters(line[3:].strip())
                        # Reset the item ready to insert
                        item_ready_to_insert = True
                    except:
                        inventor_country = None
                        item_ready_to_insert = True
                        logger.error("A inventor country data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # If next line does not have a header but more text, it is the continuince of NAM
                elif not line[0:3].strip():
                    # Get the citation text from the line
                    try:
                        if multi_line_flag == "STR":
                            inventor_residence += " " +  replace_old_html_characters(line[3:].strip())
                    except:
                        logger.error("An inventor data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # If next header is found then store data and end loop
                elif line[0:4].strip() not in accepted_headers_array:

                    # Append SQL data into dictionary to be written later
                    processed_inventor.append({
                        "table_name" : "uspto.INVENTOR_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "FirstName" : inventor_first_name,
                        "LastName" : inventor_last_name,
                        "City" : inventor_city,
                        "State" : inventor_state,
                        "Country" : inventor_country,
                        "Nationality" : inventor_nationality,
                        "Residence" : inventor_residence,
                        "FileName" : args_array['file_name']
                    })

                    # Reset all the variables associated so they don't get reused
                    inventor_first_name = None
                    inventor_last_name = None
                    inventor_city = None
                    inventor_nationality = None
                    inventor_country = None
                    inventor_residence = None
                    inventor_state = None

                    #print processed_inventor

                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # Break the foreign patent citation loop
                    data_parse_completed = True

        # Inventor
        elif line[0:4] == "ASSG":

            # Init the position
            position = 1
            multi_line_flag = ''
            # Array of expected header strings
            accepted_headers_array = ["ASSG", "NAM", "STR", "CTY", "STA", "CNT", "ZIP", "COD", "ITX"]
            # Init ready to insert to false to first flag is not caught
            item_ready_to_insert = False
            # Set loop flag for finished reading inventors to false
            data_parse_completed = False
            # Ensure that all variables that are optionally included will be set.
            asn_orgname = None
            asn_city = None
            asn_state = None
            asn_country = None
            asn_role = None

            # loop through all inventors
            while data_parse_completed == False:

                # Read next line
                line = data_reader.readline()
                #print line

                # If the inventor tag is found then append last set of data
                if line[0:4] == "ASSG":

                    # The data collection is complete and should be appended
                    if item_ready_to_insert == True:

                        # Append SQL data into dictionary to be written later
                        try:
                            # Append SQL data into dictionary to be written later
                            processed_assignee.append({
                                "table_name" : "uspto.ASSIGNEE_G",
                                "GrantID" : document_id,
                                "Position" : position,
                                "OrgName" : asn_orgname,
                                "Role" : asn_role,
                                "City" : asn_city,
                                "State" : asn_state,
                                "Country" : asn_country,
                                "FileName" : args_array['file_name']
                            })

                            # Reset all variables so they don't get reused.
                            asn_orgname = None
                            asn_city = None
                            asn_state = None
                            asn_country = None
                            asn_role = None

                            #print processed_assignee

                            # Increment the position
                            position += 1
                            # Reset the item ready to insert
                            item_ready_to_insert = False

                        except Exception as e:
                            print "Data missing from assignee for grant id : " + document_id + " in url: " + args_array['url_links']
                            logger.error("Some data was missing from assignee reference data for grant id: " + document_id + " in url: " + args_array['url_link'])
                            # Reset the item ready to insert
                            item_ready_to_insert = False


                # Get and pase the name of the inventor
                elif line[0:3] == "NAM":
                    # Get the citation text from the line
                    try:
                        asn_orgname = replace_old_html_characters(line[3:].strip())
                        multi_line_flag = "NAM"
                    except:
                        asn_orgname = None
                        logger.error("An assignee data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the street of inventor
                elif line[0:3] == "CTY":
                    # Get the citation text from the line
                    try:
                        asn_city = replace_old_html_characters(line[3:].strip())
                    except:
                        asn_city = None
                        logger.error("A assignee data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the state of inventor
                elif line[0:3] == "STA":
                    # Get the citation text from the line
                    try:
                        asn_state = replace_old_html_characters(line[3:].strip())
                    except:
                        asn_state = None
                        logger.error("A assignee data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the state of inventor
                elif line[0:3] == "COD":
                    # Get the citation text from the line
                    try:
                        asn_role = replace_old_html_characters(line[3:].strip())
                    except:
                        asn_role = None
                        logger.error("A assignee data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # get the country of inventor
                elif line[0:3] == "CNT":
                    # Get the citation text from the line
                    try:
                        asn_country = replace_old_html_characters(line[3:].strip())
                        # Reset the item ready to insert
                        item_ready_to_insert = True
                    except:
                        asn_country = None
                        item_ready_to_insert = True
                        logger.error("A inventor data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # If next line does not have a header but more text, it is the continuince of NAM
                elif not line[0:3].strip():
                    # Get the citation text from the line
                    try:
                        if multi_line_flag == "NAM":
                            asn_orgname += " " +  replace_old_html_characters(line[3:].strip())
                    except:
                        logger.error("An assignee data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # if next header is found store and end loop
                elif line[0:4].strip() not in accepted_headers_array:

                    # Append SQL data into dictionary to be written later
                    processed_assignee.append({
                        "table_name" : "uspto.ASSIGNEE_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : asn_orgname,
                        "Role" : asn_role,
                        "City" : asn_city,
                        "State" : asn_state,
                        "Country" : asn_country,
                        "FileName" : args_array['file_name']
                    })

                    # Ensure that all variables that are optionally included will be set.
                    asn_orgname = None
                    asn_city = None
                    asn_state = None
                    asn_country = None
                    asn_role = None

                    #print processed_assignee
                    position += 1
                    # Set the next_line_loaded_already flag to True
                    next_line_loaded_already = True
                    # Break the foreign patent citation loop
                    data_parse_completed = True

        # Get the Agent
        elif line[0:4] == "LREP":

            # Init the position
            position = 1
            # Set the accepted expected headers allowed in agent dataset
            accepted_headers_array = ["LREP", "FRM", "FR2", "AAT", "AGT", "ATT", "REG", "NAM", "STR", "CTY", "STA", "CNT", "ZIP"]
            # Set loop flag for finished reading inventors to false
            data_parse_completed = False
            # Ensure that all variables that are optionally included will be set.
            agent_country = None
            agent_orgname = None

            # loop through all inventors
            while data_parse_completed == False:

                # Read next line
                line = data_reader.readline()
                #print line

                # Get the firm name from line
                if line[0:3] == "FRM":
                    # Get the firm name from the line
                    try:
                        agent_orgname = replace_old_html_characters(line[3:].strip())
                    except:
                        agent_orgname = None
                        logger.error("An agent data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                # Get the principle attorney name and append to array
                elif line[0:3] == "FR2":
                    # Get the citation text from the line
                    try:
                        agent_name = replace_old_html_characters(line[3:].strip()).split(";")
                        agent_first_name = agent_name[1].strip()
                        agent_last_name = agent_name[0].strip()
                    except:
                        agent_first_name = None
                        agent_last_name = None
                        logger.error("A agent data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # Append SQL data into dictionary to be written later
                    processed_agent.append({
                        "table_name" : "uspto.AGENT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : agent_orgname,
                        "LastName" : agent_last_name,
                        "FirstName" : agent_first_name,
                        "Country" : agent_country,
                        "FileName" : args_array['file_name']
                    })

                    # Reset all variables so they don't get reused.
                    agent_first_name = None
                    agent_last_name = None
                    agent_country = None
                    agent_orgname = None

                    #print processed_agent

                    # Increment position
                    position += 1

                # Get associate attorney name and append to dataset
                elif line[0:3] == "AAT":
                    # Get the citation text from the line
                    try:
                        agent_name = replace_old_html_characters(line[3:].strip()).split(";")
                        agent_first_name = agent_name[1].strip()
                        agent_last_name = agent_name[0].strip()
                    except:
                        agent_first_name = None
                        agent_last_name = None
                        logger.error("A agent data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # Append SQL data into dictionary to be written later
                    processed_agent.append({
                        "table_name" : "uspto.AGENT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : agent_orgname,
                        "LastName" : agent_last_name,
                        "FirstName" : agent_first_name,
                        "Country" : agent_country,
                        "FileName" : args_array['file_name']
                    })

                    # Reset all variables so they don't get reused.
                    agent_first_name = None
                    agent_last_name = None
                    agent_country = None
                    agent_orgname = None

                    #print processed_agent

                    # Increment position
                    position += 1

                # Get Agent's name and append to dataset
                elif line[0:3] == "AGT":
                    # Get the citation text from the line
                    try:
                        agent_name = replace_old_html_characters(line[3:].strip()).split(";")
                        agent_first_name = agent_name[1].strip()
                        agent_last_name = agent_name[0].strip()
                    except:
                        agent_first_name = None
                        agent_last_name = None
                        logger.error("A agent data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # Append SQL data into dictionary to be written later
                    processed_agent.append({
                        "table_name" : "uspto.AGENT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : agent_orgname,
                        "LastName" : agent_last_name,
                        "FirstName" : agent_first_name,
                        "Country" : agent_country,
                        "FileName" : args_array['file_name']
                    })

                    # Reset all variables so they don't get reused.
                    agent_first_name = None
                    agent_last_name = None
                    agent_country = None
                    agent_orgname = None

                    #print processed_agent

                    # Increment position
                    position += 1

                # Get Agent's name and append to dataset
                elif line[0:3] == "ATT":
                    # Get the citation text from the line
                    try:
                        agent_name = replace_old_html_characters(line[3:].strip()).split(";")
                        agent_first_name = agent_name[1].strip()
                        agent_last_name = agent_name[0].strip()
                    except:
                        agent_first_name = None
                        agent_last_name = None
                        logger.error("A agent data error occurred for grant_id: " + document_id + " in link: " + args_array['url_link'])

                    # Append SQL data into dictionary to be written later
                    processed_agent.append({
                        "table_name" : "uspto.AGENT_G",
                        "GrantID" : document_id,
                        "Position" : position,
                        "OrgName" : agent_orgname,
                        "LastName" : agent_last_name,
                        "FirstName" : agent_first_name,
                        "Country" : agent_country,
                        "FileName" : args_array['file_name']
                    })

                    # Reset all variables so they don't get reused.
                    agent_first_name = None
                    agent_last_name = None
                    agent_country = None
                    agent_orgname = None

                    #print processed_agent

                    # Increment position
                    position += 1

                # Else check if the header is from the next datatype
                elif line[0:4].strip() not in accepted_headers_array:
                    # End the while loop for agent data
                    data_parse_completed = True
                    next_line_loaded_already = True

    # Close the open data reader file being read from
    data_reader.close()
    # Close all the open .csv files
    close_csv_files(args_array)

    # Set a flag file_processed to ensure that the bulk insert succeeds
    file_processed = True

    # If data is to be inserted as bulk csv files, then call the sql function
    if args_array['database_insert_mode'] == 'bulk':
        file_processed = args_array['database_connection'].load_csv_bulk_data(args_array, logger)

    if file_processed:
        # Send the information to write_process_log to have log file rewritten to "Processed"
        write_process_log(args_array)
        if "csv" not in args_array['command_args']:
            # Close all the open csv files
            delete_csv_files(args_array)

        # Print message to stdout and log
        print '[Processed .bat or .txt File. Total time:{0}  Time: {1}]'.format(time.time()-start_time, time.strftime('%c'))


# Function used to extract data from XML4 formatted patent applications
def extract_XML4_application(raw_data, args_array):

    url_link = args_array['url_link']
    uspto_xml_format = args_array['uspto_xml_format']

    # Define required arrays
    processed_application = []
    processed_priority_claims = []
    processed_assignee = []
    processed_applicant = []
    processed_agent = []
    processed_inventor = []
    processed_usclass = []
    processed_intclass = []
    processed_cpcclass = []

    # Set process start time
    start_time = time.time()

    # Print start message to stdout
    #print '- Starting to extract xml in USPTO application format ' + uspto_xml_format + " Start time: " + time.strftime("%c")

    # Pass the raw data into Element tree xml object
    patent_root = ET.fromstring(raw_data)

    # Start extract XML data
    for r in patent_root.findall('us-bibliographic-data-application'):

        # Get basic document ID information
        pr = r.find('publication-reference')
        pub_doc = pr.find('document-id')
        try: pub_country = pub_doc.findtext('country')
        except:
            pub_country = None
        try:
            document_id = pub_doc.findtext('doc-number')
            document_id = fix_patent_number(document_id)
        except:
            document_id = None
            logger.error("No Patent Number was found for: " + url_link)
        try: kind = pub_doc.findtext('kind')
        except: kind = None
        try: pub_date = return_formatted_date(pub_doc.findtext('date'), args_array, document_id)
        except: pub_date = None

        # Get application reference data
        ar = r.find('application-reference')
        try: app_type = ar.attrib['appl-type']
        except: app_type = None
        app_doc = ar.find('document-id')
        try: app_country = app_doc.findtext('country')
        except: app_country = None
        try: app_no = app_doc.findtext('doc-number')
        except: app_no = None
        try: app_date = return_formatted_date(app_doc.findtext('date'), args_array, document_id)
        except: app_date = None
        # Get series code
        try: series_code = r.findtext('us-application-series-code')
        except: series_code = None

        # Get priority Claims
        pcs = r.find('priority-claims')
        if pcs is not None:
            for pc in pcs.findall('priority-claim'):
                try: pc_sequence = strip_leading_zeros(pc.attrib['sequence'])
                except: pc_sequence = None
                try: pc_kind = pc.attrib['kind']
                except: pc_kind = None
                try: pc_country = pc.findtext('country')
                except: pc_country = None
                try: pc_doc_num = pc.findtext('doc-number')
                except: pc_doc_num = None
                try: pc_date = return_formatted_date(pc.findtext('date'), args_array, document_id)
                except: pc_date = None

                # Append SQL data into dictionary to be written later
                processed_priority_claims.append({
                    "table_name" : "uspto.FOREIGNPRIORITY_A",
                    "ApplicationID" : app_no,
                    "Position" : pc_sequence,
                    "Kind" : pc_kind,
                    "Country" : pc_country,
                    "DocumentID" : pc_doc_num,
                    "PriorityDate" : pc_date,
                    "FileName" : args_array['file_name']
                })

                #print processed_priority_claims

        # Get International classifcation data
        ics = r.find('classifications-ipcr')
        # Init position for int classifications
        position = 1
        if ics is not None:
            # Get all international classification
            for icc in ics.findall('classification-ipcr'):

                for x in icc.getchildren():
                    if(check_tag_exists(x,'section')): i_class_sec = x.text
                    if(check_tag_exists(x,'class')): i_class = x.text
                    if(check_tag_exists(x,'subclass')): i_subclass = x.text
                    if(check_tag_exists(x,'main-group')): i_class_mgr = x.text
                    if(check_tag_exists(x,'subgroup')): i_class_sgr = x.text

                # Append SQL data into dictionary to be written later
                processed_intclass.append({
                    "table_name" : "uspto.INTCLASS_A",
                    "ApplicationID" : app_no,
                    "Position" : position,
                    "Section" : i_class_sec,
                    "Class" : i_class,
                    "SubClass" : i_subclass,
                    "MainGroup" : i_class_mgr,
                    "SubGroup" : i_class_sgr,
                    "FileName" : args_array['file_name']
                })

                # Increment position
                position += 1

                #print processed_intclass

        # Get US Classification data
        nc = r.find('classification-national')
        # Init position
        position = 1
        if nc is not None:
            try: n_class_country = nc.findtext('country')
            except: n_class_country = None
            try: n_class_info = nc.findtext('main-classification')
            except: n_class_info = None
            try: n_class_main, n_subclass = return_class(n_class_info)
            except:
                n_class_main = None
                n_subclass = None

            # Append SQL data into dictionary to be written later
            processed_usclass.append({
                "table_name" : "uspto.USCLASS_A",
                "ApplicationID" : app_no,
                "Position" : position,
                "Class" : n_class_main,
                "SubClass" : n_subclass,
                "FileName" : args_array['file_name']
            })

            # Increment position
            position += 1

            # TODO: find an instance of futher classification to parse
            if nc.findall('further-classification') is not None:
                nat_class_fur_root = nc.findall('further-classification')
                for n in nat_class_fur_root:
                    try: n_class_info = n.text
                    except: n_class_info = None
                    try: n_class_main, n_subclass = return_class(n_class_info)
                    except:
                        n_class_main = None
                        n_subclass = None

                    # Append SQL data into dictionary to be written later
                    processed_usclass.append({
                        "table_name" : "uspto.USCLASS_A",
                        "ApplicationID" : app_no,
                        "Position" : position,
                        "Class" : n_class_main,
                        "SubClass" : n_subclass,
                        "FileName" : args_array['file_name']
                    })

                    # Increment position
                    position += 1

        # Get CPC Classification data
        cpc_class_element = r.find('classifications-cpc')
        # Init position
        position = 1
        if cpc_class_element is not None:
            main_cpc_class_element = cpc_class_element.find('main-cpc')
            if main_cpc_class_element is not None:
                for cpc_class_item in main_cpc_class_element.findall('classification-cpc'):
                    try: cpc_section = cpc_class_item.findtext('section')
                    except: cpc_section = None
                    try: cpc_class = cpc_class_item.findtext('class')
                    except: cpc_class = None
                    try: cpc_subclass = cpc_class_item.findtext('subclass')
                    except: cpc_subclass = None
                    try: cpc_mgr = cpc_class_item.findtext('main-group')
                    except: cpc_mgr = None
                    try: cpc_sgr = cpc_class_item.findtext('subgroup')
                    except: cpc_sgr = None

                    # Append SQL data into dictionary to be written later
                    processed_cpcclass.append({
                        "table_name" : "uspto.CPCCLASS_A",
                        "ApplicationID" : app_no,
                        "Position" : position,
                        "Section" : cpc_section,
                        "Class" : cpc_class,
                        "SubClass" : cpc_subclass,
                        "MainGroup" : cpc_mgr,
                        "SubGroup" : cpc_sgr,
                        "FileName" : args_array['file_name']
                    })

                    # Increment position
                    position += 1

            further_cpc_class = cpc_class_element.find('further-cpc')
            if further_cpc_class is not None:
                for cpc_class_item in further_cpc_class.findall('classification-cpc'):
                    try: cpc_section = cpc_class_item.findtext('section')
                    except: cpc_section = None
                    try: cpc_class = cpc_class_item.findtext('class')
                    except: cpc_class = None
                    try: cpc_subclass = cpc_class_item.findtext('subclass')
                    except: cpc_subclass = None
                    try: cpc_mgr = cpc_class_item.findtext('main-group')
                    except: cpc_mgr = None
                    try: cpc_sgr = cpc_class_item.findtext('subgroup')
                    except: cpc_sgr = None

                    # Append SQL data into dictionary to be written later
                    processed_cpcclass.append({
                        "table_name" : "uspto.CPCCLASS_A",
                        "ApplicationID" : app_no,
                        "Position" : position,
                        "Section" : cpc_section,
                        "Class" : cpc_class,
                        "SubClass" : cpc_subclass,
                        "MainGroup" : cpc_mgr,
                        "SubGroup" : cpc_sgr,
                        "FileName" : args_array['file_name']
                    })

                    # Increment position
                    position += 1

        # Get the title of the application
        try:
            title = r.findtext('invention-title')
        except:
            title = None
            logger.error("Title not Found for :" + url_link + " Application ID: " + app_no)

        # Get number of claims
        try: claims_num = r.findtext('number-of-claims')
        except: claims_num = None

        # Get number of figure, drawings
        nof = r.find('figures')
        try: number_of_drawings = nof.findtext('number-of-drawing-sheets')
        except: number_of_drawings = None
        try: number_of_figures = nof.findtext('number-of-figures')
        except: number_of_figures = None


        # Increment position
        position = 1
        # Get Associated party data
        parties_element = r.find('us-parties')
        if parties_element is not None:
            applicant_element = parties_element.find('us-applicants')
            # Get Applicant data
            for applicant_item in applicant_element.findall('us-applicant'):
                if(applicant_item.find('addressbook') != None):
                    try: applicant_orgname = applicant_item.find('addressbook').findtext('orgname')
                    except: applicant_orgname = None
                    try: applicant_role = applicant_item.find('addressbook').findtext('role')
                    except: applicant_role = None
                    try: applicant_city = applicant_item.find('addressbook').find('address').findtext('city')
                    except: applicant_city = None
                    try: applicant_state = applicant_item.find('addressbook').find('address').findtext('state')
                    except: applicant_state = None
                    try: applicant_country = applicant_item.find('addressbook').find('address').findtext('country')
                    except: applicant_country = None
                    try: applicant_first_name = applicant_item.find('addressbook').findtext('first-name')
                    except: applicant_first_name = None
                    try: applicant_last_name = applicant_item.find('addressbook').findtext('last-name')
                    except: applicant_last_name = None

                    # Append SQL data into dictionary to be written later
                    processed_applicant.append({
                        "table_name" : "uspto.APPLICANT_A",
                        "ApplicationID" : app_no,
                        "Position" : position,
                        "OrgName" : applicant_orgname,
                        "FirstName" : applicant_first_name,
                        "LastName" : applicant_last_name,
                        "City" : applicant_city,
                        "State" : applicant_state,
                        "Country" : applicant_country,
                        "FileName" : args_array['file_name']
                    })

                    # Increment position
                    position += 1

                    #print processed_applicant

        # Get the inventor data element
        invs = parties_element.find('inventors')
        # Init position
        position = 1
        # Get all inventors
        for inv in invs.findall("inventor"):
            if(inv.find('addressbook') != None):
                try: inventor_first_name = inv.find('addressbook').findtext('first-name')
                except: inventor_first_name = None
                try: inventor_last_name = inv.find('addressbook').findtext('last-name')
                except: inventor_last_name = None
                try: inventor_city = inv.find('addressbook').find('address').findtext('city')
                except: inventor_city = None
                try: inventor_state = inv.find('addressbook').find('address').findtext('state')
                except: inventor_state = None
                try: inventor_country = inv.find('addressbook').find('address').findtext('country')
                except: inventor_country = None
                try: inventor_nationality = inv.find('nationality').findtext('country')
                except: inventor_nationality = None
                try: inventor_residence = inv.find('residence').findtext('country')
                except: inventor_residence = None

                # Append SQL data into dictionary to be written later
                processed_inventor.append({
                    "table_name" : "uspto.INVENTOR_A",
                    "ApplicationID" : app_no,
                    "Position" : position,
                    "FirstName" : inventor_first_name,
                    "LastName" : inventor_last_name,
                    "City" : inventor_city,
                    "State" : inventor_state,
                    "Country" : inventor_country,
                    "Nationality" : inventor_nationality,
                    "Residence" : inventor_residence,
                    "FileName" : args_array['file_name']
                })

                # Increment position
                position += 1

                #print processed_inventor

        # Init position
        position = 1
        # Get agent data
        #TODO Find if available in application ??? Where
        agents_element = parties_element.find('agents')
        if agents_element is not None:
            for agent_item in agents_element.findall('agent'):
                try: asn_sequence = agent_item.attrib['sequence']
                except: asn_sequence = None
                if(agent_item.find('addressbook') != None):
                    try: atn_orgname = agent_item.find('addressbook').findtext('orgname')
                    except: atn_orgname = None
                    try: atn_last_name = agent_item.find('addressbook').findtext('last-name')
                    except: atn_last_name = None
                    try: atn_first_name = agent_item.find('addressbook').findtext('first-name')
                    except: atn_first_name = None
                    try: atn_country = agent_item.find('addressbook').find('address').findtext('country')
                    except: atn_country = None

                    # Append SQL data into dictionary to be written later
                    processed_agent.append({
                        "table_name" : "uspto.AGENT_A",
                        "ApplicationID" : app_no,
                        "Position" : position,
                        "OrgName" : atn_orgname,
                        "LastName" : atn_last_name,
                        "FirstName" : atn_first_name,
                        "Country" : atn_country,
                        "FileName" : args_array['file_name']
                    })

                    # Increment position
                    position += 1

                    #print processed_agent

        # Get assignee data
        # Init position
        position += 1
        assignee_element = r.find('assignees')
        if assignee_element is not None:
            for assignee_item in assignee_element.findall('assignee'):
                if(applicant_item.find('addressbook') != None):
                    try: assignee_orgname = applicant_item.find('addressbook').findtext('orgname')
                    except: assignee_orgname = None
                    try: assignee_role = applicant_item.find('addressbook').findtext('role')
                    except: assignee_role = None
                    try: assignee_city = applicant_item.find('addressbook').find('address').findtext('city')
                    except: assignee_city = None
                    try: assignee_state = applicant_item.find('addressbook').find('address').findtext('state')
                    except: assignee_state = None
                    try: assignee_country = applicant_item.find('addressbook').find('address').findtext('country')
                    except: assignee_country = None

                    # Append SQL data into dictionary to be written later
                    processed_assignee.append({
                        "table_name" : "uspto.ASSIGNEE_A",
                        "ApplicationID" : app_no,
                        "Position" : position,
                        "OrgName" : assignee_orgname,
                        "Role" : assignee_role,
                        "City" : assignee_city,
                        "State" : assignee_state,
                        "Country" : assignee_country,
                        "FileName" : args_array['file_name']
                    })

                    # Increment position
                    position += 1

                    #print processed_assignee

    # Get abstract data
    # Find the abstract
    try:
        abstract_element = patent_root.find('abstract')
        abstract = return_element_text(abstract_element)
    except: abstract = None
    #print abstract

    # Append SQL data into dictionary to be written later
    processed_application.append({
            "table_name": "uspto.APPLICATION",
            "ApplicationID" : app_no,
            "PublicationID" : document_id,
            "AppType" : app_type,
            "Title" :  title,
            "FileDate" : app_date,
            "PublishDate" : pub_date,
            "Kind" : kind,
            "USSeriesCode" : series_code,
            "Abstract" : abstract,
            "ClaimsNum" : claims_num,
            "DrawingsNum" : number_of_drawings,
            "FiguresNum" : number_of_figures,
            "FileName" : args_array['file_name']
        })


    # Return a dictionary of the processed_ data arrays
    return {
        "processed_application" : processed_application,
        "processed_priority_claims": processed_priority_claims,
        "processed_assignee" : processed_assignee,
        "processed_agent" : processed_agent,
        "processed_inventor" : processed_inventor,
        "processed_usclass" : processed_usclass,
        "processed_intclass" : processed_intclass,
        "processed_cpcclass" : processed_cpcclass,
    }

# Function used to extract data from XML1 formatted patent applications
def extract_XML1_application(raw_data, args_array):

    url_link = args_array['url_link']
    uspto_xml_format = args_array['uspto_xml_format']

    # Define required arrays
    processed_application = []
    processed_assignee = []
    processed_agent = []
    processed_inventor = []
    processed_usclass = []
    processed_intclass = []
    processed_cpcclass = []

    # Set process start time
    start_time = time.time()

    # Print start message to stdout
    print '- Starting to extract xml in USPTO application format ' + uspto_xml_format + " Start time: " + time.strftime("%c")

    #print raw_data
    # Pass the xml into Element tree object
    document_root = ET.fromstring(raw_data)
    r = document_root.find('subdoc-bibliographic-information')

    # Get and fix the document_id data
    di = r.find('document-id')
    try:
        # This document ID is NOT application number
        document_id = di.findtext('doc-number')
    except:
        document_id = None
        logger.error("No Patent Number was found for: " + url_link)
    try: kind = di.findtext('kind-code')
    except: kind = None
    try: pub_date = return_formatted_date(di.findtext('document-date'), args_array, document_id)
    except: pub_date = None
    try: app_type = r.findtext('publication-filing-type')
    except: app_type = None

    # Get application filing data
    ar = r.find('domestic-filing-data')
    try:
        app_no = ar.find('application-number').findtext('doc-number')
    except: app_no = None
    try: app_date = return_formatted_date(ar.findtext('filing-date'), args_array, document_id)
    except: app_date = None
    try: series_code = ar.findtext('application-number-series-code')
    except: series_code = None

    technical_information_element = r.find('technical-information')
    # Get international classification data
    ic = technical_information_element.find('classification-ipc')
    if ic is not None:

        # Init position
        position = 1

        icm = ic.find('classification-ipc-primary')
        #TODO: regex the class found into class, subclass and other
        #TODO: find out what maingrou and subgroup are found in this file format
        try: i_class_sec, i_class, i_subclass, i_class_mgr, i_class_sgr = return_international_class(icm.findtext('ipc'))
        except:
            i_class_sec = None
            i_class = None
            i_subclass = None
            i_class_mgr = None
            i_class_sgr = None
            logger.warning("Malformed international class found in application ID: " + document_id +  " in file: " + url_link)

        # Append SQL data into dictionary to be written later
        processed_intclass.append({
            "table_name" : "uspto.INTCLASS_A",
            "ApplicationID" : app_no,
            "Position" : position,
            "Section" : i_class_sec,
            "Class" : i_class,
            "SubClass" : i_subclass,
            "MainGroup" : i_class_mgr,
            "SubGroup" : i_class_sgr,
            "FileName" : args_array['file_name']
        })

        # Increment Position
        position += 1

        #print processed_intclass

        ics = ic.findall('classification-ipc-secondary')
        if ics is not None:
            try: i_class_sec, i_class, i_subclass, i_class_mgr, i_class_sgr = return_international_class(icm.findtext('ipc'))
            except:
                i_class_sec = None
                i_class = None
                i_subclass = None
                i_class_mgr = None
                i_class_sgr = None
                logger.warning("Malformed international class found in application ID: " + document_id +  " in file: " + url_link)

            # Append SQL data into dictionary to be written later
            processed_intclass.append({
                "table_name" : "uspto.INTCLASS_A",
                "ApplicationID" : app_no,
                "Position" : position,
                "Section" : i_class_sec,
                "Class" : i_class,
                "SubClass" : i_subclass,
                "MainGroup" : i_class_mgr,
                "SubGroup" : i_class_sgr,
                "FileName" : args_array['file_name']
            })

            # Increment position
            position += 1

            #print processed_intclass

    # Get US classification data
    nc = technical_information_element.find('classification-us')
    if nc is not None:
        # init position
        position = 1

        uspc = nc.find('classification-us-primary').find('uspc')
        try: n_class_main = uspc.findtext('class')
        except: n_class_main = None
        try: n_subclass = uspc.findtext('subclass')
        except: n_subclass = None

        # Append SQL data into dictionary to be written later
        processed_usclass.append({
            "table_name" : "uspto.USCLASS_A",
            "ApplicationID" : app_no,
            "Position" : position,
            "Class" : n_class_main,
            "SubClass" : n_subclass,
            "FileName" : args_array['file_name']
        })

        # Increment position
        position += 1

        #print processed_usclass

        us_classification_secondary_element =  nc.find('classification-us-secondary')
        if us_classification_secondary_element is not None:
            uspc = us_classification_secondary_element.find('uspc')
            try: n_class_main = uspc.findtext('class')
            except: n_class_main = None
            try: n_subclass = uspc.findtext('subclass')
            except: n_subclass = None

            # Append SQL data into dictionary to be written later
            processed_usclass.append({
                "table_name" : "uspto.USCLASS_A",
                "ApplicationID" : app_no,
                "Position" : position,
                "Class" : n_class_main,
                "SubClass" : n_subclass,
                "FileName" : args_array['file_name']
            })

            # Increment position
            position += 1

            #print processed_usclass

    # Get invention title
    try: title = technical_information_element.findtext('title-of-invention')[0:500]
    except: title = None

    # Get inventor data
    iv = r.find('inventors')
    if iv is not None:

        # Init position
        position = 1

        for inventor in iv.findall('first-named-inventor'):
            n = inventor.find('name')
            try: inventor_first_name = n.findtext('given-name')
            except: inventor_first_name = None
            try: inventor_last_name = n.findtext('family-name')
            except: inventor_last_name = None

            res = inventor.find('residence')
            residence_us = res.find('residence-us')
            if residence_us is not None:
                try: inventor_city = residence_us.findtext('city')
                except: inventor_city = None
                try: inventor_state = residence_us.findtext('state')
                except: inventor_state = None
                try: inventor_country = residence_us.findtext('country-code')
                except: inventor_country = None
            residence_non_us = res.find('residence-non-us')
            if residence_non_us is not None:
                try: inventor_city = residence_non_us.findtext('city')
                except: inventor_city = None
                try: inventor_state = residence_non_us.findtext('state')
                except: inventor_state = None
                try: inventor_country = residence_non_us.findtext('country-code')
                except: inventor_country = None

            # Append SQL data into dictionary to be written later
            processed_inventor.append({
                "table_name" : "uspto.INVENTOR_A",
                "ApplicationID" : app_no,
                "Position" : position,
                "FirstName" : inventor_first_name,
                "LastName" : inventor_last_name,
                "City" : inventor_city,
                "State" : inventor_state,
                "Country" : inventor_country,
                "FileName" : args_array['file_name']
            })

            # Increment position
            position += 1

            #print processed_inventor

        # For all secordary inventors
        for inventor in iv.findall('inventor'):
            n = inventor.find('name')
            try: inventor_first_name = n.findtext('given-name')
            except: inventor_first_name = None
            try: inventor_last_name = n.findtext('family-name')
            except: inventor_last_name = None

            res = inventor.find('residence')
            residence_us = res.find('residence-us')
            if residence_us is not None:
                try: inventor_city = residence_us.findtext('city')
                except: inventor_city = None
                try: inventor_state = residence_us.findtext('state')
                except: inventor_state = None
                try: inventor_country = residence_us.findtext('country-code')
                except: inventor_country = None
            residence_non_us = res.find('residence-non-us')
            if residence_non_us is not None:
                try: inventor_city = residence_non_us.findtext('city')
                except: inventor_city = None
                try: inventor_state = residence_non_us.findtext('state')
                except: inventor_state = None
                try: inventor_country = residence_non_us.findtext('country-code')
                except: inventor_country = None

            # Append SQL data into dictionary to be written later
            processed_inventor.append({
                "table_name" : "uspto.INVENTOR_A",
                "ApplicationID" : app_no,
                "Position" : position,
                "FirstName" : inventor_first_name,
                "LastName" : inventor_last_name,
                "City" : inventor_city,
                "State" : inventor_state,
                "Country" : inventor_country,
                "FileName" : args_array['file_name']
            })

            # Increment position
            position += 1

            #print processed_inventor

    assignee_element  = r.find('assignee')
    if assignee_element is not None:
        # init position
        position = 1

        try: asn_role = assignee_element.findtext('assignee-type')
        except: asn_role = None
        on = assignee_element.find('organization-name')
        try: asn_orgname = return_element_text(on)
        except: asn_orgname = None
        ad = assignee_element.find('address')
        try: asn_city = ad.findtext('city')
        except: asn_city = None
        try: asn_state = ad.findtext('state')
        except: asn_state = None
        try: asn_country = ad.find('country').findtext('country-code')
        except: asn_country = None

        # Append SQL data into dictionary to be written later
        processed_assignee.append({
            "table_name" : "uspto.ASSIGNEE_A",
            "ApplicationID" : app_no,
            "Position" : position,
            "OrgName" : asn_orgname,
            "Role" : asn_role,
            "City" : asn_city,
            "State" : asn_state,
            "Country" : asn_country,
            "FileName" : args_array['file_name']
        })

        # increment position
        position += 1

    # Find the agent elements
    agent_element = r.find('correspondence-address')
    # init position
    position = 1
    if agent_element is not None:
        try: agent_orgname = agent_element.findtext('name-1')
        except: agent_orgname = None
        try: agent_orgname += agent_element.findtext('name-2')
        except: agent_orgname = None
        try:
            adresss_element = agent_element.find('address')
            if address_element is not None:
                try: agent_city = adresss_element.findtext('city')
                except: agent_city = None
                try: agent_state = adresss_element.findtext('state')
                except: agent_state = None
                try: agent_country = adresss_element.find('country').findtext('country-code')
                except: agent_country = None
        except:
            agent_city = None
            agent_state = None
            agent_country = None

        # Append SQL data into dictionary to be written later
        processed_agent.append({
            "table_name" : "uspto.AGENT_A",
            "ApplicationID" : app_no,
            "Position" : position,
            "OrgName" : agent_orgname,
            "Country" : agent_country,
            "FileName" : args_array['file_name']
        })

        # increment position
        position += 1

    # Find the abstract of the application
    try: abstract = return_element_text(document_root.find('subdoc-abstract')).replace("\n", " ").strip()
    except: abstract = None

    # Append SQL data into dictionary to be written later
    processed_application.append({
            "table_name" : "uspto.APPLICATION",
            "ApplicationID" : app_no,
            "PublicationID" : document_id,
            "AppType" : app_type,
            "Title" :  title,
            "FileDate" : app_date,
            "PublishDate" : pub_date,
            "Kind" : kind,
            "USSeriesCode" : series_code,
            "Abstract" : abstract,
            "FileName" : args_array['file_name']
        })

    #print processed_application

    # Return a dictionary of the processed_ data arrays
    return {
        "processed_application" : processed_application,
        "processed_assignee" : processed_assignee,
        "processed_agent" : processed_agent,
        "processed_inventor" : processed_inventor,
        "processed_usclass" : processed_usclass,
        "processed_intclass" : processed_intclass,
        "processed_cpcclass" : processed_cpcclass
    }

# Function used to open the required csv files and create a csv.DictWrite object
# for each one.  This function also creates arrays of table column names for each table
# and returns both the csv.DictWrite and table column arrays back to the args_array.
def open_csv_files(file_type, file_name, csv_directory):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Create an array of files to append to
    field_names_array = {}
    csv_writer_array = {}

    # Define filename for csv file
    csv_file_name = file_name + '.csv'

    # If the grant CSV file will be written
    if file_type == "grant":

        # Create array of field names for each application table and append to array to be passed back with args array
        field_names_array['grant'] = ['GrantID', 'IssueDate', 'Kind', 'USSeriesCode', 'Title', 'Abstract', 'Claims', 'ClaimsNum', 'DrawingsNum', 'FiguresNum', 'GrantLength', 'ApplicationID', 'FileDate', 'AppType', 'FileName']
        field_names_array['applicant'] = ['GrantID', 'Position', 'OrgName', 'FirstName', 'LastName', 'City', 'State', 'Country', 'FileName']
        field_names_array['examiner'] = ['GrantID', 'Position', 'LastName', 'FirstName', 'Department', 'FileName']
        field_names_array['agent'] = ['GrantID', 'Position', 'OrgName', 'LastName', 'FirstName', 'Country', 'FileName']
        field_names_array['assignee'] = ['GrantID', 'Position', 'OrgName', 'Role', 'City', 'State', 'Country', 'FileName']
        field_names_array['inventor'] = ['GrantID', 'Position', 'FirstName', 'LastName', 'City', 'State', 'Country', 'Nationality', 'Residence', 'FileName']
        field_names_array['gracit'] = ['GrantID', 'Position', 'CitedID', 'Kind', 'Name', 'Date', 'Country', 'Category', 'FileName']
        field_names_array['forpatcit'] = ['GrantID', 'Position', 'CitedID', 'Kind', 'Name', 'Date', 'Country', 'Category', 'FileName']
        field_names_array['nonpatcit'] = ['GrantID', 'Position', 'Citation', 'Category', 'FileName']
        field_names_array['usclass'] = ['GrantID','Position', 'Class', 'SubClass', 'Malformed', 'FileName']
        field_names_array['intclass'] = ['GrantID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']
        field_names_array['cpcclass'] = ['GrantID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']

        # Define all the dictionary arrays to hold writers and filenames
        csv_writer_array['grant'] = {}
        csv_writer_array['applicant'] = {}
        csv_writer_array['examiner'] = {}
        csv_writer_array['agent'] = {}
        csv_writer_array['assignee'] = {}
        csv_writer_array['inventor'] = {}
        csv_writer_array['gracit'] = {}
        csv_writer_array['forpatcit'] = {}
        csv_writer_array['nonpatcit'] = {}
        csv_writer_array['usclass'] = {}
        csv_writer_array['intclass'] = {}
        csv_writer_array['cpcclass'] = {}

        # Define all the .csv filenames fullpath and append to array
        csv_writer_array['grant']['csv_file_name'] = csv_directory + 'CSV_G/grant_' + csv_file_name
        csv_writer_array['applicant']['csv_file_name'] = csv_directory + 'CSV_G/applicant_' + csv_file_name
        csv_writer_array['examiner']['csv_file_name'] = csv_directory + 'CSV_G/examiner_' + csv_file_name
        csv_writer_array['agent']['csv_file_name'] = csv_directory + 'CSV_G/agent_' + csv_file_name
        csv_writer_array['assignee']['csv_file_name'] = csv_directory + 'CSV_G/assignee_' + csv_file_name
        csv_writer_array['inventor']['csv_file_name'] = csv_directory + 'CSV_G/inventor_' + csv_file_name
        csv_writer_array['gracit']['csv_file_name'] = csv_directory + 'CSV_G/gracit_' + csv_file_name
        csv_writer_array['forpatcit']['csv_file_name'] = csv_directory + 'CSV_G/forpatcit_' + csv_file_name
        csv_writer_array['nonpatcit']['csv_file_name'] = csv_directory + 'CSV_G/nonpatcit_' + csv_file_name
        csv_writer_array['usclass']['csv_file_name'] = csv_directory + 'CSV_G/usclass_' + csv_file_name
        csv_writer_array['intclass']['csv_file_name'] = csv_directory + 'CSV_G/intclass_' + csv_file_name
        csv_writer_array['cpcclass']['csv_file_name'] = csv_directory + 'CSV_G/cpcclass_' + csv_file_name

        # Define all the dictionary arrays to hold writers and filenames
        csv_writer_array['grant']['file'] = open(csv_writer_array['grant']['csv_file_name'], 'w')
        csv_writer_array['applicant']['file'] = open(csv_writer_array['applicant']['csv_file_name'], 'w')
        csv_writer_array['examiner']['file'] = open(csv_writer_array['examiner']['csv_file_name'], 'w')
        csv_writer_array['agent']['file'] = open(csv_writer_array['agent']['csv_file_name'], 'w')
        csv_writer_array['assignee']['file'] = open(csv_writer_array['assignee']['csv_file_name'], 'w')
        csv_writer_array['inventor']['file'] = open(csv_writer_array['inventor']['csv_file_name'], 'w')
        csv_writer_array['gracit']['file'] = open(csv_writer_array['gracit']['csv_file_name'], 'w')
        csv_writer_array['forpatcit']['file'] = open(csv_writer_array['forpatcit']['csv_file_name'], 'w')
        csv_writer_array['nonpatcit']['file'] = open(csv_writer_array['nonpatcit']['csv_file_name'], 'w')
        csv_writer_array['usclass']['file'] = open(csv_writer_array['usclass']['csv_file_name'], 'w')
        csv_writer_array['intclass']['file'] = open(csv_writer_array['intclass']['csv_file_name'], 'w')
        csv_writer_array['cpcclass']['file'] = open(csv_writer_array['cpcclass']['csv_file_name'], 'w')

        # Open all CSV files to write to and append to array
        csv_writer_array['grant']['csv_writer'] = csv.DictWriter(csv_writer_array['grant']['file'], fieldnames = field_names_array['grant'], delimiter = '|')
        csv_writer_array['applicant']['csv_writer'] = csv.DictWriter(csv_writer_array['applicant']['file'], fieldnames = field_names_array['applicant'], delimiter = '|')
        csv_writer_array['examiner']['csv_writer'] = csv.DictWriter(csv_writer_array['examiner']['file'], fieldnames = field_names_array['examiner'], delimiter = '|')
        csv_writer_array['agent']['csv_writer'] = csv.DictWriter(csv_writer_array['agent']['file'], fieldnames = field_names_array['agent'], delimiter = '|')
        csv_writer_array['assignee']['csv_writer'] = csv.DictWriter(csv_writer_array['assignee']['file'], fieldnames = field_names_array['assignee'], delimiter = '|')
        csv_writer_array['inventor']['csv_writer'] = csv.DictWriter(csv_writer_array['inventor']['file'], fieldnames = field_names_array['inventor'], delimiter = '|')
        csv_writer_array['gracit']['csv_writer'] = csv.DictWriter(csv_writer_array['gracit']['file'], fieldnames = field_names_array['gracit'], delimiter = '|')
        csv_writer_array['forpatcit']['csv_writer'] = csv.DictWriter(csv_writer_array['forpatcit']['file'], fieldnames = field_names_array['forpatcit'], delimiter = '|')
        csv_writer_array['nonpatcit']['csv_writer'] = csv.DictWriter(csv_writer_array['nonpatcit']['file'], fieldnames = field_names_array['nonpatcit'], delimiter = '|')
        csv_writer_array['usclass']['csv_writer'] = csv.DictWriter(csv_writer_array['usclass']['file'], fieldnames = field_names_array['usclass'], delimiter = '|')
        csv_writer_array['intclass']['csv_writer'] = csv.DictWriter(csv_writer_array['intclass']['file'], fieldnames = field_names_array['intclass'], delimiter = '|')
        csv_writer_array['cpcclass']['csv_writer'] = csv.DictWriter(csv_writer_array['cpcclass']['file'], fieldnames = field_names_array['cpcclass'], delimiter = '|')

        # Write the header to each file
        csv_writer_array['grant']['csv_writer'].writeheader()
        csv_writer_array['applicant']['csv_writer'].writeheader()
        csv_writer_array['examiner']['csv_writer'].writeheader()
        csv_writer_array['agent']['csv_writer'].writeheader()
        csv_writer_array['assignee']['csv_writer'].writeheader()
        csv_writer_array['inventor']['csv_writer'].writeheader()
        csv_writer_array['gracit']['csv_writer'].writeheader()
        csv_writer_array['forpatcit']['csv_writer'].writeheader()
        csv_writer_array['nonpatcit']['csv_writer'].writeheader()
        csv_writer_array['usclass']['csv_writer'].writeheader()
        csv_writer_array['intclass']['csv_writer'].writeheader()
        csv_writer_array['cpcclass']['csv_writer'].writeheader()

    # If the application CSV file will be written
    elif file_type == "application":

        # Create array of field names for each application table and append to array to be passed back with args array
        field_names_array['application'] = ['ApplicationID', 'PublicationID', 'FileDate', 'Kind', 'USSeriesCode', 'AppType', 'PublishDate', 'Title', 'Abstract', 'ClaimsNum', 'DrawingsNum', 'FiguresNum', 'FileName']
        field_names_array['agent'] = ['ApplicationID', 'Position', 'OrgName', 'LastName', 'FirstName', 'Country', 'FileName']
        field_names_array['assignee'] = ['ApplicationID', 'Position', 'OrgName', 'Role', 'City', 'State', 'Country', 'FileName']
        field_names_array['inventor'] = ['ApplicationID', 'Position', 'FirstName', 'LastName', 'City', 'State', 'Country', 'Nationality', 'Residence', 'FileName']
        field_names_array['usclass'] = ['ApplicationID','Position', 'Class', 'SubClass', 'Malformed', 'FileName']
        field_names_array['intclass'] = ['ApplicationID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']
        field_names_array['cpcclass'] = ['ApplicationID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']
        field_names_array['foreignpriority'] = ['ApplicationID', 'DocumentID', 'Position', 'Kind', 'Country', 'PriorityDate', 'FileName']

        # Define all the dicionaries to hold the csv data
        csv_writer_array['application'] = {}
        csv_writer_array['agent'] = {}
        csv_writer_array['assignee'] = {}
        csv_writer_array['inventor'] = {}
        csv_writer_array['usclass'] = {}
        csv_writer_array['intclass'] = {}
        csv_writer_array['cpcclass'] = {}
        csv_writer_array['foreignpriority'] = {}

        # Define all the .csv filenames fullpath and append to array
        csv_writer_array['application']['csv_file_name'] = csv_directory + 'CSV_A/application_' + csv_file_name
        csv_writer_array['agent']['csv_file_name'] = csv_directory + 'CSV_A/agent_' + csv_file_name
        csv_writer_array['assignee']['csv_file_name'] = csv_directory + 'CSV_A/assignee_' + csv_file_name
        csv_writer_array['inventor']['csv_file_name'] = csv_directory + 'CSV_A/inventor_' + csv_file_name
        csv_writer_array['usclass']['csv_file_name'] = csv_directory + 'CSV_A/usclass_' + csv_file_name
        csv_writer_array['intclass']['csv_file_name'] = csv_directory + 'CSV_A/intclass_' + csv_file_name
        csv_writer_array['cpcclass']['csv_file_name'] = csv_directory + 'CSV_A/cpcclass_' + csv_file_name
        csv_writer_array['foreignpriority']['csv_file_name'] = csv_directory + 'CSV_A/foreignpriority_' + csv_file_name

        # Define all the dicionaries to hold the csv data
        csv_writer_array['application']['file'] = open(csv_writer_array['application']['csv_file_name'], 'w')
        csv_writer_array['agent']['file'] = open(csv_writer_array['agent']['csv_file_name'], 'w')
        csv_writer_array['assignee']['file'] = open(csv_writer_array['assignee']['csv_file_name'], 'w')
        csv_writer_array['inventor']['file'] = open(csv_writer_array['inventor']['csv_file_name'], 'w')
        csv_writer_array['usclass']['file'] = open(csv_writer_array['usclass']['csv_file_name'], 'w')
        csv_writer_array['intclass']['file'] = open(csv_writer_array['intclass']['csv_file_name'], 'w')
        csv_writer_array['cpcclass']['file'] = open(csv_writer_array['cpcclass']['csv_file_name'], 'w')
        csv_writer_array['foreignpriority']['file'] = open(csv_writer_array['foreignpriority']['csv_file_name'], 'w')

        # Open all CSV files to write to and append to array
        csv_writer_array['application']['csv_writer'] = csv.DictWriter(csv_writer_array['application']['file'], fieldnames = field_names_array['application'], delimiter = '|')
        csv_writer_array['agent']['csv_writer'] = csv.DictWriter(csv_writer_array['agent']['file'], fieldnames = field_names_array['agent'], delimiter = '|')
        csv_writer_array['assignee']['csv_writer'] = csv.DictWriter(csv_writer_array['assignee']['file'], fieldnames = field_names_array['assignee'], delimiter = '|')
        csv_writer_array['inventor']['csv_writer'] = csv.DictWriter(csv_writer_array['inventor']['file'], fieldnames = field_names_array['inventor'], delimiter = '|')
        csv_writer_array['usclass']['csv_writer'] = csv.DictWriter(csv_writer_array['usclass']['file'], fieldnames = field_names_array['usclass'], delimiter = '|')
        csv_writer_array['intclass']['csv_writer'] = csv.DictWriter(csv_writer_array['intclass']['file'], fieldnames = field_names_array['intclass'], delimiter = '|')
        csv_writer_array['cpcclass']['csv_writer'] = csv.DictWriter(csv_writer_array['cpcclass']['file'], fieldnames = field_names_array['cpcclass'], delimiter = '|')
        csv_writer_array['foreignpriority']['csv_writer'] = csv.DictWriter(csv_writer_array['foreignpriority']['file'], fieldnames = field_names_array['foreignpriority'], delimiter = '|')

        # Write header for all application csv files
        csv_writer_array['application']['csv_writer'].writeheader()
        csv_writer_array['agent']['csv_writer'].writeheader()
        csv_writer_array['assignee']['csv_writer'].writeheader()
        csv_writer_array['inventor']['csv_writer'].writeheader()
        csv_writer_array['usclass']['csv_writer'].writeheader()
        csv_writer_array['intclass']['csv_writer'].writeheader()
        csv_writer_array['cpcclass']['csv_writer'].writeheader()
        csv_writer_array['foreignpriority']['csv_writer'].writeheader()

    # Write message to stdout and log that all csv files are open
    print '[Opened all .csv files for ' + file_type + ' ' + file_name + ' storage Time: {0}]'.format(time.strftime('%c'))
    logger.info('[Opened all .csv files for ' + file_type + ' ' + file_name + ' storage Time: {0}]'.format(time.strftime('%c')))

    # Return the array
    return csv_writer_array
    #return True

# Function used to close all .csv files in array
def close_csv_files(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Print message to stdout and log
    print '[Cleaning up .csv files... ]'
    logger.info('[Cleaning up .csv files... ]')

    # Loop through each file in array of open csv files
    for key, csv_file in args_array['csv_file_array'].items():
        try:
            # Close file being written to
            csv_file['file'].close()
            # Print message to stdout and log
            print 'Closed .csv file: ' + csv_file['csv_file_name'] + ' Time: {0}'.format(time.strftime('%c'))
            logger.info('Closed .csv file: ' + csv_file['csv_file_name'] + ' Time: {0}'.format(time.strftime('%c')))
            # Remove csv file from the CSV directory if 'csv' not in args_array['command_args']
        except Exception as e:
            # Print exception information to file
            print 'Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c'))
            logger.error('Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c')))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
            traceback.print_exc()

# Function used to close all csv files in array
def delete_csv_files(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Print message to stdout and log
    print '[Cleaning up .csv files... ]'
    logger.info('[Cleaning up .csv files... ]')

    # Loop through each file in array of open csv files
    for key, csv_file in args_array['csv_file_array'].items():
        try:
            # Remove csv file from the CSV directory if 'csv' not in args_array['command_args']
            if os.path.exists(csv_file['csv_file_name']):
                os.remove(csv_file['csv_file_name'])
            # Print message to stdout and log
            print 'Removed .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c'))
            logger.info('Removed .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c')))
        except Exception as e:
            # Print exception information to file
            print 'Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c'))
            logger.error('Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c')))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
            traceback.print_exc()



# Function used to store grant data in CSV and/or database
def store_grant_data(processed_data_array, args_array):

    # Extract critical variables from args_array
    uspto_xml_format = args_array['uspto_xml_format']
    database_connection = args_array['database_connection']
    file_name = args_array['file_name']

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Set process start time
    start_time = time.time()

    # If the argument specified to store data into csv file or csv is needed for bulk database insertion
    if "csv" in args_array["command_args"] or ("database" in args_array['command_args'] and args_array['database_insert_mode'] == "bulk"):

        # Process all the collected grant data for one patent record into csv file
        # Using the already opened csv.csv.DictWriter object stored in args array.
        # Table name must be appended to the dictionary for later processing
        if "processed_grant" in processed_data_array and len(processed_data_array['processed_grant']):
            for data_item in processed_data_array['processed_grant']:
                # Print start message to stdout and log
                print '- Starting to write {0} to .csv file {1} for document: {2}. Start Time: {3}'.format(args_array['document_type'], file_name, data_item['GrantID'], time.strftime("%c"))
                #logger.info('- Starting to write {0} to .csv file {1} for document: {2}. Start Time: {3}'.format(args_array['document_type'], file_name, data_item['GrantID'], time.strftime("%c")))
                # Move the table name to temp variable and remove from table
                table_name = data_item['table_name']
                del data_item['table_name']
                # Try catch is to avoid failing the whole file when
                # htmlentity characters found or other error occurs
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['grant']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['grant']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_applicant" in processed_data_array and len(processed_data_array['processed_applicant']):
            for data_item in processed_data_array['processed_applicant']:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['applicant']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['applicant']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_examiner" in processed_data_array and len(processed_data_array['processed_examiner']):
            for data_item in processed_data_array['processed_examiner']:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['examiner']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['examiner']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_agent" in processed_data_array and len(processed_data_array['processed_agent']):
            for data_item in processed_data_array["processed_agent"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['agent']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['agent']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_assignee" in processed_data_array and len(processed_data_array['processed_assignee']):
            for data_item in processed_data_array["processed_assignee"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['assignee']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['assignee']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_inventor" in processed_data_array and len(processed_data_array['processed_inventor']):
            for data_item in processed_data_array["processed_inventor"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['inventor']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['inventor']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_gracit" in processed_data_array and len(processed_data_array['processed_gracit']):
            for data_item in processed_data_array["processed_gracit"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['gracit']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['gracit']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_nonpatcit" in processed_data_array and len(processed_data_array['processed_nonpatcit']):
            for data_item in processed_data_array["processed_nonpatcit"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['nonpatcit']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['nonpatcit']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_forpatcit" in processed_data_array and len(processed_data_array['processed_forpatcit']):
            for data_item in processed_data_array["processed_forpatcit"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['forpatcit']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['forpatcit']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_usclass" in processed_data_array and len(processed_data_array['processed_usclass']):
            for data_item in processed_data_array["processed_usclass"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['usclass']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['usclass']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_intclass" in processed_data_array and len(processed_data_array['processed_intclass']):
            for data_item in processed_data_array["processed_intclass"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['intclass']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['intclass']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_cpcclass" in processed_data_array and len(processed_data_array['processed_cpcclass']):
            for data_item in processed_data_array["processed_cpcclass"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['cpcclass']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['cpcclass']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['GrantID'], table_name, time.strftime("%c")))
                    traceback.print_exc()


    # If command arg is set to put data into database
    elif "database" in args_array["command_args"] and args_array['database_insert_mode'] == "each":

        # Print start message to stdout
        print '- Starting to write {0} to database. Start Time: {1}'.format(file_name, time.strftime("%c"))

        # Reset the start time
        start_time = time.time()

        # Strip the processed_grant item off the array and process it first
        processed_grant = processed_data_array['processed_grant']
        del processed_data_array['processed_grant']
        for item in processed_grant:
            # Store table name for stdout
            args_array['table_name'] = item['table_name']
            args_array['document_id'] = item['GrantID']
            # Build query and pass to database loader
            database_connection.load(build_sql_insert_query(item, args_array), args_array, logger)

        # Loop throught the processed_data_array and create sql queries and execute them
        for key, value in processed_data_array.items():
            for item in value:
                # Store table name for stdout
                args_array['table_name'] = item['table_name']
                args_array['document_id'] = item['GrantID']
                # Build query and pass to database loader
                database_connection.load(build_sql_insert_query(item, args_array), args_array, logger)

# Funtion to store patent application data to csv and/or database
# TODO: change function to append to csv files until threshhold reached
# then dump CSV to database in batch, and erase CSV file if -csv flag not set.
def store_application_data(processed_data_array, args_array):

    # Extract critical variables from args_array
    uspto_xml_format = args_array['uspto_xml_format']
    database_connection = args_array['database_connection']
    file_name = args_array['file_name']

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Set process start time
    start_time = time.time()

    # If the argument specified to store data into csv file or csv is needed for bulk database insertion
    if "csv" in args_array["command_args"] or ("database" in args_array['command_args'] and args_array['database_insert_mode'] == "bulk"):



        # Process all the collected application data for one patent record into .csv file
        # Using the already opened csv.DictWriter object stored in args array.
        if "processed_application" in processed_data_array and len(processed_data_array['processed_application']):
            for data_item in processed_data_array["processed_application"]:
                # Print start message to stdout and log
                print '- Starting to write {0} to .csv file {1} for document: {2}. Start Time: {3}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], time.strftime("%c"))
                #logger.info('- Starting to write {0} to .csv file {1} for document: {2}. Start Time: {3}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], time.strftime("%c")))
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['application']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['application']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_agent" in processed_data_array and len(processed_data_array['processed_agent']):
            for data_item in processed_data_array["processed_agent"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['agent']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['agent']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_assignee" in processed_data_array and len(processed_data_array['processed_assignee']):
            for data_item in processed_data_array["processed_assignee"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['assignee']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['assignee']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_inventor" in processed_data_array and len(processed_data_array['processed_inventor']):
            for data_item in processed_data_array["processed_inventor"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['inventor']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['inventor']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_usclass" in processed_data_array and len(processed_data_array['processed_usclass']):
            for data_item in processed_data_array["processed_usclass"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['usclass']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['usclass']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_intclass" in processed_data_array and len(processed_data_array['processed_intclass']):
            for data_item in processed_data_array["processed_intclass"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['intclass']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['intclass']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_cpcclass" in processed_data_array and len(processed_data_array['processed_cpcclass']):
            for data_item in processed_data_array["processed_cpcclass"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['cpcclass']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['cpcclass']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()
        if "processed_foreignpriority" in processed_data_array and len(processed_data_array['processed_foreignpriority']):
            for data_item in processed_data_array["processed_foreignpriority"]:
                table_name = data_item['table_name']
                del data_item['table_name']
                try:
                    # Write the dictionary of document data to .csv file
                    args_array['csv_file_array']['foreignpriority']['csv_writer'].writerow(data_item)
                    # Append the table onto the array
                    args_array['csv_file_array']['foreignpriority']['table_name'] = table_name
                except Exception as e:
                    print '- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c"))
                    logger.info('- Error writing {0} to .csv file {1} for document: {2} into table {3}. Start Time: {4}'.format(args_array['document_type'], file_name, data_item['ApplicationID'], table_name, time.strftime("%c")))
                    traceback.print_exc()

    elif "database" in args_array["command_args"] and args_array['database_insert_mode'] == "each":


        # Print start message to stdout
        print '- Starting to write {0} to database. Start Time: {1}'.format(file_name, time.strftime("%c"))

        # Reset the start time
        start_time = time.time()

        # Strip the processed_grant item off the array and process it first
        processed_application = processed_data_array['processed_application']
        del processed_data_array['processed_application']
        for item in processed_application:
            args_array['table_name'] = item['table_name']
            args_array['document_id'] = item['ApplicationID']
            # Build query and pass to database loader
            database_connection.load(build_sql_insert_query(item, args_array), args_array, logger)

        # Loop throught the processed_data_array and create sql queries and execute them
        for key, value in processed_data_array.items():
            for item in value:
                args_array['table_name'] = item['table_name']
                args_array['document_id'] = item['ApplicationID']
                database_connection.load(build_sql_insert_query(item, args_array), args_array, logger)


# This function accepts a table name and a dictionary with keys as column names and values as data.
# It builds an sql query out of this array.
def build_sql_insert_query(insert_data_array, args_array):

    uspto_xml_format = args_array['uspto_xml_format']

    # Set a length counter used to find when the last item is appended to query string
    array_length_counter = 1
    length_of_array = len(insert_data_array) - 1
    # Pass the table name to variable
    table_name = insert_data_array['table_name']
    # Pop the table name off the array to be stored into database
    del insert_data_array['table_name']

    sql_query_string = "INSERT INTO " + table_name + " "
    sql_column_string = "("
    sql_value_string = " VALUES ("
    # Concatenate the list of keys and values to sql format
    for key, value in insert_data_array.items():

        # Don't escape values that are None (NULL)
        if value is not None and isinstance(value, int) == False:
            # escape all values for sql insertion
            value = escape_value_for_sql(str(value.encode('utf-8')))
            # Since postgresql uses `$` as delimiter, must  strip from first and last char
            value = value.strip("$").replace("$$$", "$").replace("$$", "$")

        # If the last item in the array then append line without comma at end
        if length_of_array == array_length_counter:
            sql_column_string += key
            # Check for None value and append
            if value == None:
                sql_value_string += 'NULL'
            else:
                # PostgreSQL strings will be escaped slightly different than MySQL
                if args_array['database_type'] == 'postgresql':
                    sql_value_string += "$$" + str(value)+ "$$"
                elif args_array['database_type'] == 'mysql':
                    sql_value_string += '"' + str(value) + '"'
        # If not the last item then append with comma
        else:
            sql_column_string += key + ", "
            # Check if value is None
            if value == None:
                sql_value_string +=  'NULL,'
            else:
                if args_array['database_type'] == 'postgresql':
                    sql_value_string +=  "$$" + str(value) + "$$,"
                elif args_array['database_type'] == 'mysql':
                    sql_value_string += '"' + str(value) + '",'
        array_length_counter += 1
    # Add the closing bracket
    sql_column_string += ") "
    sql_value_string += ");"

    # Concatnate the pieces of the query
    sql_query_string += sql_column_string + sql_value_string
    # Return the query string
    return sql_query_string

# Download a link into temporary memory and return filename
def download_zip_file(link):

    # Set process start time
    start_time = time.time()

    # Try to download the zip file to temporary location
    try:
        print '[Downloading .zip file: {0}]'.format(link)
        file_name = urllib.urlretrieve(link)[0]
        #uuid_path = ''.join([random.choice(string.letters + string.digits) for i in range(10)])
        #file_name = os.path.join(args_array['temp_dirpath'], uuid_path)
        #file_name = open(temp_path, 'w')
        #response = urllib2.urlopen(link)
        #temp_file.write(response.read())
        #temp_file.close()
        print '[Downloaded .zip file: {0} Time:{1} Finish Time: {2}]'.format(file_name,time.time()-start_time, time.strftime("%c"))
        # Return the filename
        return file_name
    except Exception as e:
        print 'Downloading  contents of ' + link + ' failed...'
        traceback.print_exc()


# Function to route the extraction of raw data from a link
def process_link_file(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Download the file and append temp location to args array
    args_array['temp_zip_file_name'] = download_zip_file(args_array['url_link'])

    #print args_array['uspto_xml_format']

    # Process the contents of file baed on type
    if args_array['uspto_xml_format'] == "gAPS":
        process_APS_grant_content(args_array)
    elif args_array['uspto_xml_format'] == "aXML1" or args_array['uspto_xml_format'] == "aXML4":
        process_XML_application_content(args_array)
    elif args_array['uspto_xml_format'] == "gXML2" or args_array['uspto_xml_format'] == "gXML4":
        process_XML_grant_content(args_array)

    print "Finished the data storage process for contents of: " + args_array['url_link'] + " Finished at: " + time.strftime("%c")


# Function opens the zip file for XML based patent grant files and parses, inserts to database
# and writes log file success
def process_XML_grant_content(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # If csv file insertion is required, then open all the files
    # into args_array
    if "csv" in args_array['command_args'] or ("database" in args_array['command_args'] and args_array['database_insert_mode'] == "bulk"):
        args_array['csv_file_array'] = open_csv_files(args_array['document_type'], args_array['file_name'], args_array['csv_directory'])

    # Process zip file by getting .dat or .txt file and .xml filenames
    start_time = time.time()

    # extract the zipfile to read it
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
                processed_data_array = extract_data_router(xml_string, args_array)
                # Call function to write data to csv or database
                store_grant_data(processed_data_array, args_array)

                # reset the xml string
                xml_string = ''

            # This is used to append lines of file when inside single patent grant
            elif patent_xml_started == True:
                # Check which type of encoding should be used to fix the line string
                xml_string += replace_new_html_characters(line)

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
                processed_data_array = extract_data_router(xml_string, args_array)
                # Call function to write data to csv or database
                store_grant_data(processed_data_array, args_array)

                # reset the xml string
                xml_string = ''

            # This is used to append lines of file when inside single patent grant
            elif patent_xml_started == True:
                # Check which type of encoding should be used to fix the line string
                xml_string += replace_old_html_characters(line)

    # Close the .xml file being read from
    xml_file.close()
    # Close all the open .csv files being written to
    close_csv_files(args_array)

    # Set a flag file_processed to ensure that the bulk insert succeeds
    file_processed = True

    # If data is to be inserted as bulk csv files, then call the sql function
    if args_array['database_insert_mode'] == 'bulk':
        file_processed = args_array['database_connection'].load_csv_bulk_data(args_array, logger)

    if file_processed:
        # Send the information to write_process_log to have log file rewritten to "Processed"
        write_process_log(args_array)
        if "csv" not in args_array['command_args']:
            # Delete all the open csv files
            delete_csv_files(args_array)

    # Print message to stdout and log
    print '[Loaded {0} data for {1} into database. Time:{2} Finished Time: {3} ]'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c"))
    logger.info('Loaded {0} data for {1} into database. Time:{2} Finished Time: {3}'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c")))


# Function opens the zip file for XML based patent application files and parses, inserts to database
# and writes log file success
def process_XML_application_content(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # If csv file insertion is required, then open all the files
    # into args_array
    if "csv" in args_array['command_args'] or ("database" in args_array['command_args'] and args_array['database_insert_mode'] == "bulk"):
        args_array['csv_file_array'] = open_csv_files(args_array['document_type'], args_array['file_name'], args_array['csv_directory'])

    # Process zip file by getting .dat or .txt file and .xml filenames
    start_time = time.time()

    zip_file = zipfile.ZipFile(args_array['temp_zip_file_name'],'r')
    for name in zip_file.namelist():
        #print name
        if '.xml' in name:
            xml_file_name = name
            print '[xml file found. Filename:{0}]'.format(xml_file_name)

    # Open the file to read lines out of
    xml_file = zip_file.open(xml_file_name, 'r')
    # Remove the temp files
    urllib.urlcleanup()
    #os.remove(file_name)
    zip_file.close()

    # create variables needed to parse the file
    xml_string = ''
    patent_xml_started = False
    # read through the file and append into groups of string.
    # Send the finished strings to be parsed
    # Use uspto_xml_format to determine file contents and parse accordingly
    if args_array['uspto_xml_format'] == "aXML4":

        # Loop through all lines in the xml file
        for line in xml_file.readlines():

            # This identifies the start of well formed XML segment for patent
            # application bibliographic information
            if "<us-patent-application" in line:

                patent_xml_started = True
                xml_string += line

            # This identifies end of well-formed XML segement for single patent
            # application bibliographic information
            elif "</us-patent-application" in line:

                patent_xml_started = False
                xml_string += line

                # Call the function extract data
                processed_data_array = extract_data_router(xml_string, args_array)
                # Call function to write data to csv or database
                store_application_data(processed_data_array, args_array)

                # reset the xml string
                xml_string = ''

            # This is used to append lines of file when inside single patent grant
            elif patent_xml_started == True:
                xml_string += replace_new_html_characters(line)

    elif args_array['uspto_xml_format'] == "aXML1":

        line_count = 1

        # Loop through all lines in the xml file
        for line in xml_file.readlines():

            # This identifies the start of well formed XML segment for patent
            # application bibliographic information
            if "<patent-application-publication" in line:

                patent_xml_started = True
                xml_string += line

            # This identifies end of well-formed XML segement for single patent
            # application bibliographic information
            elif "</patent-application-publication" in line:

                patent_xml_started = False
                xml_string +=  line

                # Call the function extract data
                processed_data_array = extract_data_router(xml_string, args_array)
                # Call function to write data to csv or database
                store_application_data(processed_data_array, args_array)

                # reset the xml string
                xml_string = ''

            # This is used to append lines of file when inside single patent grant
            elif patent_xml_started == True:
                xml_string += replace_old_html_characters(line)

    # Close the .xml file being read from
    xml_file.close()
    # Close the all the .csv files being written to
    close_csv_files(args_array)

    # Set a flag file_processed to ensure that the bulk insert succeeds
    file_processed = True

    # If data is to be inserted as bulk csv files, then call the sql function
    if args_array['database_insert_mode'] == 'bulk':
        file_processed = args_array['database_connection'].load_csv_bulk_data(args_array, logger)

    if file_processed:
        # Send the information to write_process_log to have log file rewritten to "Processed"
        write_process_log(args_array)
        if "csv" not in args_array['command_args']:
            # Close all the open csv files
            delete_csv_files(args_array)

    # Print message to stdout and log
    print '[Loaded {0} data for {1} into database. Time:{2} Finished Time: {3} ]'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c"))
    logger.info('Loaded {0} data for {1} into database. Time:{2} Finished Time: {3}'.format(args_array['document_type'], args_array['url_link'], time.time() - start_time, time.strftime("%c")))

# Convert strings to utf-8 for csv file insertion
def utf_8_encoder(line):
    return line.encode('utf-8')

#Converts html encoding to hex encoding for database insertion
def replace_new_html_characters(line):

    # Use a regex replacement to replace all html encoded strings
    try:
        # Finally use regex to replace anything that looks like an html entity with nothing
        pattern = re.compile(r"\&#x[A-Za-z0-9]{1,}\;")
        line = re.sub(pattern, "", line)

        # Replace all tab characters before putting into tab delimted .csv
        line = line.replace("|", "")
        line = line.replace("\n", "")
        line = line.replace("\t", "")

    except Exception as e:
        print line
        traceback.print_exc()
        # Print exception information to file
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

    # Return the line without any html encoded strings.
    return line


#Converts html encoding to hex encoding for database insertion
def replace_old_html_characters(line):

    # Use a regex replacement to replace all html encoded strings
    try:
        # Finally use regex to replace anything that looks like an html entity with nothing
        pattern = re.compile(r"\&[A-Za-z0-9]{1,}\;")
        line = re.sub(pattern, "", line)

        # Replace all tab characters before putting into tab delimted .csv
        line = line.replace("|", "")
        line = line.replace("\n", "")
        line = line.replace("\t", "")

    except Exception as e:
        print line
        traceback.print_exc()
        # Print exception information to file
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

    # Return the line without any html encoded strings.
    return line


#Converts html encoding to hex encoding for database insertion
def replace_old_html_characters_old_version(line):

    # TODO: make more logical replacements of characters
    # (1) replace all left and write quotes with just general quote characters
    # (2) replace all link breaks within the whole document.
    # (3) What is the regex doing?  Maybe waste of resources.
    # (4) Check all values are accurate
    # (5) consider replacing with just plain text for searchable

    replacement_dict = {
        '&amp;' : '&#x26;',
        '&lt;' : '&#x3C;',
        '&gt;' : '&#x3E;', #&#62;',
        '&quot;' : '&#x22;', #&#34;',
        '&lsquo;' : '&#x2018;', #&#8216;',
        '&rsquo;' : '&#x2019;', #&#8217;',
        '&ldquo;' : '&#x201C;', #&#8220;',
        '&rdquo;' : '&#x201D;', #&#8221;',
        '&sbquo;' : '&#x201A;', #&#8218;',
        '&bdquo;' : '&#x201E;', #&#8222;',
        '&ndash;' : '&#x2013;', #&#8211;',
        '&mdash;' : '&#x2014;', #&#8212;',
        '&minus;' : '&#8722;',
        '&times;': '&#215;',
        '&divide;' : '&#247;',
        '&copy;' : '&#169;',
        '&lsaquo;' : '&#x2039;',
        '&rsaquo;' : '&#x203A;',
        '&num;' : '&#035;',
        '&excl;' : '&#033;',
        '&apos;' : '&#039;',
        '&lsqb;' : "[",
        '&rsqb;' : "]",
        '&#169;' : '&#xa9;',
        '&reg;' : '&#xae;',
        '&trade;' : '&#x2122;',
        "&plus;" : '+',
        "&quest;" : "?",
        "&equals;" : "=",
        '&rcub;' : '&#x7d;',
        '&lcub;' : '&#x7b;',
        '&square;' : '&#x20DE;',
        '&bgr;' : '',
        '&frac18;' : '&#x215B;',
        '&frac12; ' : '&#x00BD; ',
        '&frac14;' : '&#x00BC;',
        '&Ecirc;' : '&#x00CA;',
        '&Aacute;' : '&#x00C1;',
        '&shy;' : '&#x00AD;',
        '&ntilde;' : '&#x00F1;',
        '&tilde;' : '&#x02DC;',
        '&laquo;' : '&#x00AB;',
        '&raquo;' : '&#x00BB;',
        '&Iacute;' : '&#x00CD;',
        '&igrave;' : '&#x00EC;',
        '&emsp;' : '&#x2003;',
        '&eth;' : '&#x00F0;',
        '&AElig;' : '&#x00C6;',
        '&yen;' : '&#x00A5;',
        '&iexcl;' : '&#x00A1;',
        '&Aring;' : '&#x00C5;',
        '&yacute;' : '&#x00FD;',
        '&icirc;' : '&#x00EE;',
        '&micro;' : '&#x00B5;',
        '&auml;' : '&#x00E4;',
        '&Oslash;' : '&#x00D8;',
        '&Otilde;' : '&#x00D5;',
        '&Ccedil;' : '&#x00C7;',
        '&egrave;' : '&#x00E8;',
        '&oacute;' : '&#x00F3;',
        '&atilde;' : '&#x00E3;',
        '&not;' : '&#x00AC;',
        '&sup1;' : '&#x00B9;',
        '&Yacute;' : '&#x00DD;',
        '&THORN;' : '&#x00DE;',
        '&euml;' : '&#x00EB;',
        '&Igrave;' : '&#x00CC;',
        '&ETH;' : '&#x00D0;',
        '&iuml;' : '&#x00EF;',
        '&frac34;' : '&#x00BE;',
        '&nbsp;' : '&#x00A0;',
        '&sup2;' : '&#x00B2;',
        '&Icirc;' : '&#x00CE;',
        '&Auml;' : '&#x00C4;',
        '&Ouml;' : '&#x00D6;',
        '&yuml;' : '&#x00FF;',
        '&eacute;' : '&#x00E9;',
        '&Egrave;' : '&#x00C8;',
        '&copy;' : '&#x00A9;',
        '&pound;' : '&#x00A3;',
        '&agrave;' : '&#x00E0;',
        '&Atilde;' : '&#x00C3;',
        '&para;' : '&#x00B6;',
        '&deg;' : '&#x00B0;',
        '&middot;' : '&#x00B7;',
        '&thorn;' : '&#x00FE;',
        '&Euml;' : '&#x00CB;',
        '&ensp;' : '&#x2002;',
        '&Iuml;' : '&#x00CF;',
        '&plusmn;' : '&#x00B1;',
        '&ograve;' : '&#x00F2;',
        '&aelig;' : '&#x00E6;',
        '&ocirc;' : '&#x00F4;',
        '&uuml;' : '&#x00FC;',
        '&iquest;' : '&#x00BF;',
        '&acirc;' : '&#x00E2;',
        '&sup3;' : '&#x00B3;',
        '&Eacute;' : '&#x00C9;',
        '&Ntilde;' : '&#x00D1;',
        '&ecirc;' : '&#x00EA;',
        '&oslash;' : '&#x00F8;',
        '&aacute;' : '&#x00E1;',
        '&Agrave;' : '&#x00C0;',
        '&Oacute;' : '&#x00D3;',
        '&sect;' : '&#x00A7;',
        '&otilde;' : '&#x00F5;',
        '&iacute;' : '&#x00ED;',
        '&cent;' : '&#x00A2;',
        '&Ocirc;' : '&#x00D4;',
        '&mdash;' : '&#x2014;',
        '&aring;' : '&#x00E5;',
        '&frac12;' : '&#x00BD;',
        '&Ograve;' : '&#x00D2;',
        '&szlig;' : '&#x00DF;',
        '&ccedil;' : '&#x00E7;',
        '&Uuml;' : '&#x00DC;',
        '&Acirc;' : '&#x00C2;',
        '&brvbar;' : '&#x00A6;',
        '&commat;' : "",
        '&lE;' : "",
        '&mgr;' : "",
        '&angst;' : "A",
        '&ohgr;' : "",
        '&Dgr;' : "",
        '&otilde;' : ""

    }

    # Replace known html entities
    for key, value in replacement_dict.items():
        #print key, value
        line = line.replace(key, value)
        #print line

    #line = set(string.printable)
    # convert to unicode and strip unprintable characters via ASCII
    line = unicode(line, errors = 'ignore')

    # replace further known entities using library
    line =  re.sub('&(%s);' % '|'.join(name2codepoint),
            lambda m: unichr(name2codepoint[m.group(1)]), line)

    try:
        # further replace known xml char replace to ascii
        line = line.encode('ascii', 'xmlcharrefreplace')
    except Exception as e:
        print line
        traceback.print_exc()
        # Print exception information to file
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

    # Finally use regex to replace anything that looks like an html entity with nothing
    pattern = re.compile(r"\&[A-Za-z0-9]{1,}\;")
    line = re.sub(pattern, "", line)

    # Replace all tab characters before putting into tab delimted .csv
    line = line.replace("|", "")

    # Replace blank strings with NULL for database
    if line == "":
        line = None

    return line

# get all the formats of grants and publications
def get_all_links(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

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

# This funtion accepts a line from the class text file and
# parses it and returns a dictionary to build an sql query string
def return_classification_array(line):

    # Build a class dictionary
    class_dictionary = {
        "table_name" : "uspto.CLASSIFICATION",
        "Class" : line[0:3],
        "SubClass" : line[3:9],
        "Indent" : line[9:11],
        "SubClsSqsNum" : line[11:15],
        "NextHigherSub" : line[15:21],
        "Title" : line[21:len(line)+1].strip()[0:140]
    }

    # Return the class dictionary
    return class_dictionary

# Main function for multiprocessing
def main_process(link_queue, args_array, spooling_value):

    # Check the spooling value in args_array and set a wait time
    args_array['spooling_value'] = spooling_value
    if args_array['spooling_value'] > 6:
        time.sleep((args_array['spooling_value']) * 30)
        args_array['spooling_value'] = 0

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Print message to stdout
    print 'Process {0} is starting to work! Start Time: {1}'.format(os.getpid(), time.strftime("%c"))
    # Set process start time
    process_start_time = time.time()

    # Create the database connection here so that each process uses its own connection,
    # hopefully to increase the bandwith to the database.
    if "database" in args_array["command_args"]:
        # Create a database connection for each thread processes
        database_connection = SQLProcessor.SQLProcess(database_args)
        database_connection.connect()
        args_array['database_connection'] = database_connection


    # Go through each link in the array passed in.
    while not link_queue.empty():
    #for item in link_pile:

        # Get the next item in the queue
        item = link_queue.get()
        # Separate link item into link and file_type and append to args_array for item
        args_array['url_link'] = item[0]
        args_array['uspto_xml_format'] = item[1]
        args_array['document_type'] = item[3]
        # file_name is used to keep track of the .zip base filename
        args_array['file_name'] = os.path.basename(args_array['url_link']).replace(".zip", "")

        # Set process time
        start_time = time.time()

        # Start the main processing of each link in link_pile array
        print "Processing .zip file: " + args_array['url_link'] + " Started at: " + time.strftime("%c")

        # Check if the args_array['file_name'] has previously been partially processed.
        # and if it has, then remove all records from the previous partial processing.
        database_connection.remove_previous_file_records(args_array['document_type'], args_array['file_name'], logger)

        # Call the function to collect patent data from each link
        # and store it to specified place
        try:
            process_link_file(args_array)
            # Print and log notification that one .zip package is finished
            print '[Finished processing one .zip package! Time consuming:{0} Time Finished: {1}]'.format(time.time() - start_time, time.strftime("%c"))
            logger.info('[Finished processing one .zip package! Time consuming:{0} Time Finished: {1}]'.format(time.time() - start_time, time.strftime("%c")))

        except Exception as e:
            # Print and log general fail comment
            print "Processing a file failed... " + args_array['file_name'] + " from link " + args_array['url_link'] + " at: " + time.strftime("%c")
            logger.error("Processing a file failed... " + args_array['file_name'] + " from link " + args_array['url_link'])
            # Print traceback
            traceback.print_exc()
            # Print exception information to file
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # TODO: Look at the other link_piles from other processes and continue to process
    # by moving links from another pile to this one.  May have to look at the log file,
    # check for unprocessed (will have to add "processing" flag.) and add a check before starting
    # processing to avoid collisions of link piles.  Make link_pile loop into a function and
    # then call it again.  OR... make link pile a super global, and somehow be able to check against
    # other processes and rebalance and pop off from link piles.

    # Print message that process is finished
    print '[Process {0} is finished. Time consuming:{1} Time Finished: {1}]'.format(time.time() - process_start_time, time.strftime("%c"))

def start_thread_processes(links_array, args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Define array to hold processes to multithread
    processes=[]

    # Calculate the total length of all links to collect
    total_links_count = len(links_array['grants']) + len(links_array['applications'])
    # Define how many threads should be started
    try:
        # If number_of_threads is set in args
        if "number_of_threads" in args_array["command_args"]:
            # Set number_of_threads appropriately
            # If requesting more threads than number of links to grab
            if int(args_array["command_args"]['number_of_threads']) > total_links_count:
                # Set number of threads at number of links
                number_of_threads = total_links_count
            # If number of threads acceptable
            else:
                # Set to command args number of threads
                number_of_threads = int(args_array["command_args"]['number_of_threads'])
        else:
            number_of_threads = args_array['default_threads']
    except Exception as e:
        # If there is a problem creating the number of threads set again and log the error
        number_of_threads = 10
        # Print and log general fail comment
        print "Calculating number of threads failed... "
        # Print traceback
        traceback.print_exc()
        # Print exception information to file
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

    # Create a Queue to hold link pile and share between threads
    link_queue = multiprocessing.Queue()
    # Put all the links into the queue
    #TODO write classification parser and also append to queue
    for link in links_array['grants']:
        link.append("grant")
        link_queue.put(link)
    for link in links_array['applications']:
        link.append("application")
        link_queue.put(link)

    # Calculate a wait time for each link and append to array

    # Create array to hold piles of links
    #thread_arrays = []
    # Break links array into separate arrays so that number_of_threads threads will start
    # If there are less links than desired number of threads make as many threads as len(links_array)
    #if len(links_array) < number_of_threads:
        #number_of_links_per_pile = 1
        #number_of_threads = len(links_array)
        #remainder_for_last_pile = 0
    # If there are more links per pile
    #else:
        #number_of_links_per_pile = len(links_array) / number_of_threads
        #remainder_for_last_pile = len(links_array) % number_of_threads

    # Loop through number_of_threads and cut array.  Append section to thread_arrays
    #for x in range(number_of_threads):
        # If last element then add remainder
        #if x == number_of_threads - 1:
            #thread_arrays.append(links_array[x * number_of_links_per_pile : (x * number_of_links_per_pile) + number_of_links_per_pile + remainder_for_last_pile])
        #else:
            #thread_arrays.append(links_array[x * number_of_links_per_pile : (x * number_of_links_per_pile) + number_of_links_per_pile])


    # Print and log the number of threads that are going to start
    print "Starting " + str(number_of_threads) + " process(es)... "
    logger.info("Starting " + str(number_of_threads) + " process(es)... ")

    # Loop for number_of_threads and append threads to process
    # for link_pile in thread_arrays:
    for i in range(number_of_threads):
        # Set an argument to hold the thread number for spooling up downloads.
        # Create a thread and append to list
        processes.append(multiprocessing.Process(target=main_process,args=(link_queue, args_array, i)))

    # Append the load balancer thread once to the loop
    processes.append(multiprocessing.Process(target=load_balancer_thread, args=(link_queue, args_array)))

    # Loop through and start all processes
    for p in processes:
        p.start()

    # Print to stdout and log that all initial threads have been started
    print "All " + str(number_of_threads) + " initial main process(es) have been loaded... "
    logger.info("All " + str(number_of_threads) + " initial main process(es) have been loaded... ")

    # This .join() function prevents the script from progressing further.
    for p in processes:
        p.join()


# Spool down the thread balance when load is too high
def spool_down_load_balance():

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Print to stdout and log that load balancing process starting
    print "[Calculating load balancing proccess... ]"
    logger.info("[Calculating load balacing process... ]")

    # get the count of CPU cores
    try:
        core_count = psutil.cpu_count()
    except Exception as e:
        core_count = 4
        print "Number of CPU cores could not be detected. Setting number of CPU cores to 4"
        logger.info("Number of CPU cores could not be detected. Setting number of CPU cores to 4")
        traceback.print_exc()

    # Set flag to keep loop running
    # TODO should I use a break here
    immediate_load_too_high = True
    load_check_count = 1

    # Loop while load balance is too high
    while immediate_load_too_high is True:
        # Calulate the immediate short term load balance of last minute average
        one_minute_load_average = os.getloadavg()[0] / core_count
        # If load balance is too high sleep process and print msg to stdout and log
        if one_minute_load_average > 2:
            print "Unacceptable load balance detected. Process " + os.getpid() + " taking a break..."
            logger.info("Unacceptable load balance detected. Process " + os.getpid() + " taking a break...")
            load_check_count = load_check_count + 1
            time.sleep(30)
        # Else if the thread had been sleeping for 5 minutes, start again
        elif load_check_count >= 10:
            immediate_load_too_high = False
        # If load balance is OK, then keep going
        else:
            immediate_load_too_high = False

# Load balancer thread function
def load_balancer_thread(link_queue, args_array):


    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    # Print to stdout and log that load balancing process starting
    print "[Starting load balancing proccess... ]"
    logger.info("[Starting load balacing process... ]")

    # get the count of CPU cores
    try:
        core_count = psutil.cpu_count()
        print str(core_count) + " CPU cores were detected..."
        logger.info(str(core_count) + " CPU cores were detected...")
    except Exception as e:
        core_count = 4
        print "Number of CPU cores could not be detected. Setting number of CPU cores to 4"
        logger.info("Number of CPU cores could not be detected. Setting number of CPU cores to 4")
        traceback.print_exc()

    # Sleep the balancer for 5 minutes to allow initial threads and CPU load to balance
    # at the initial number of threads
    time.sleep(300)
    # While there is still links in queue
    while not link_queue.empty():
        # Check the 15 minute average CPU load balance
        five_minute_load_average = os.getloadavg()[1] / core_count

        # If the load average is very small, start a group of new threads
        if (five_minute_load_average) < 0.75:
            # Print message and log that load balancer is starting another thread
            print "Starting another thread group due to low CPU load balance of: " + str(five_minute_load_average * 100) + "%"
            logger.info("Starting another thread group due to low CPU load balance of: " + str(five_minute_load_average * 100) + "%")
            # Start another group of threads and pass in i to stagger the downloads
            for i in range(1):
                start_new_thread = multiprocessing.Process(target=main_process,args=(link_queue, args_array, i))
                start_new_thread.start()
                time.sleep(2)
            time.sleep(300)

        # If load average less than 1 start single thread
        elif (five_minute_load_average) < 1:
            # Print message and log that load balancer is starting another thread
            print "Starting another single thread due to low CPU load balance of: " + str(five_minute_load_average * 100) + "%"
            logger.info("Starting another single thread due to low CPU load balance of: " + str(five_minute_load_average * 100) + "%")
            # Start another thread and pass in 0 to start right away
            start_new_thread = multiprocessing.Process(target=main_process,args=(link_queue, args_array, i))
            start_new_thread.start()
            time.sleep(300)



        else:
            # Print message and log that load balancer is starting another thread
            print "Reporting CPU load balance: " + str(five_minute_load_average * 100) + "%"
            logger.info("Reporting CPU load balance: " + str(five_minute_load_average * 100) + "%")
            # Sleep for another 5 minutes while
            time.sleep(300)

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


# Collect all inks from file
def collect_all_links_from_file(args_array):

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
            all_links_array = get_all_links(args_array)
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
            all_links_array = get_all_links(args_array)
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

# Check existing app structure and create it if required
def validate_existing_file_structure(args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    try:
        # Check that the structure required for the app to function are in place
        # If not then create directory structure
        for required_directory in args_array['required_directory_array']:
            if not os.path.exists(args_array['working_directory'] + required_directory):
                os.makedirs(args_array['working_directory'] + required_directory)

        # Create the log file lock and set to open.
        log_lock = open(log_lock_file, "w")
        log_lock.write("0")
        log_lock.close()

        # Print stdout and log the file structure validation process
        print "Finished creating require directory structure " + time.strftime("%c")
        logger.info('Finished creating require directory structure ' + time.strftime("%c"))

        # Return `True` that file structure has been created
        return True

    except Exception as e:
        # Log finished building all zip filepaths
        print "Failed to create require directory structure " + time.strftime("%c")
        traceback.print_exc()
        logger.error('Failed to create require directory structure ' + time.strftime("%c"))
        # Log exception error messages
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(str(e) + str(exc_type) + str(fname) + str(exc_tb.tb_lineno))
        # Return false to main function
        return False

# Reset the database using os.system command line
def reset_database(database_args, args_array):

    print "Resetting database contents...."

    try:
        # Set the variables to pass to SQL
        user = database_args['user']
        passwd = database_args['passwd']
        host = database_args['host']
        port = database_args['port']
        db = database_args['db']
        filename = args_array['database_reset_file']

        if args_array['database_type'] == "mysql":
            command = """mysql -u %s -p="%s" -h %s -P %s %s < %s""" %(user, passwd, host, port, db, filename)
            print command
            command_return_string = os.system(command)
            print command_return_string
        else:
            print "PostgreSQL cannot be automatically reset from commmand argument...."

    except Exception as e:
        traceback.print_exc()


# Parses the command argument sys.arg into command set, also encrypt password for use
def build_command_arguments(argument_array, args_array):

    # Import logger
    logger = logging.getLogger("USPTO_Database_Construction")

    try:
        # Create an array to store modified command line arguemnts
        command_args = {}

        # Pop off the first element of array because it's the application filename
        argument_array.pop(0)

        # Check that the argument array is proper length (4)
        if len(argument_array) < 1 or len(argument_array) > 4:
            # Argument length is not ok, print message and return False
            print "Command argument error [incorrect number of arguments]...."
            # Print out full argument help menu
            print build_argument_output()
            # Return false to the main function to indicate not continue
            return False

        # For loop to modify elements and strip "-" and check if arguement expected
        for i in range(len(argument_array)):
            skip = 0
            if skip + i == len(argument_array):
                break
            if argument_array[i] in args_array['allowed_args_array']:
                # Check for help menu requested
                if argument_array[i] == "-h" or argument_array[i] == "-help":
                    # Print out full argument help menu
                    print build_argument_output()
                    # Return false to the main function to indicate not continue
                    return False
                elif argument_array[i] == "-t":
                    # Check that next argument is integer between 0 and 20
                    if int(argument_array[i + 1]) > 0 and int(argument_array[i + 1]) < 31:
                        command_args['number_of_threads'] = argument_array[i + 1]
                        # Pop the value off
                        argument_array.pop(i + 1)
                        # Increment i to avoid the number of threads value
                        skip = skip + 1
                    # If the argument for number_of_threads is invalid return error
                    else:
                        # Argument length is not ok, print message and return False
                        print "Command argument error [illegal number of threads]...."
                        # Print out full argument help menu
                        print build_argument_output()
                        # Return false to the main function to indicate not continue
                        return False
                else:
                    # If the argument is expected but not other, append as key to command_args
                    command_args[argument_array[i].replace('-', '')] = True
            else:
                # Argument is not expected, print message and return False
                print "Command argument error [illegal argument]...."
                # Print out full argument help menu
                print build_argument_output()
                # Return false to the main function to indicate not continue
                return False

        # Finally correct that number_of_threads value is definitely in the array
        if "number_of_threads" not in command_args:
            command_args['number_of_threads'] = args_array['default_threads']

        # If arguments passed then return array of arguments
        return command_args

    except Exception as e:
        # Print the error to stdout
        print 'Failed to build command arguments: '
        # Print the exception to stdout
        traceback.print_exc()
        # Collect the exception information
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # Log error with creating filepath
        logger.error('Failed to build command arguments: ' + str(e) + str(exc_type) + str(fname) + str(exc_tb.tb_lineno))
        return False

def build_argument_output():
    argument_output = "\nUsage : USPTOParser.py [-t [int]] & [-csv, &| -database] | [-update]\n\n"
    # Add the description of how to run the parser
    argument_output += "USPTOParser.py requires data destination arguments (-csv, -database) when running for the first time. \n"
    argument_output += "Database credentials are defined an a dictionary in the main function for if the database flag is set. \n"
    argument_output += "After the script has been run the first time, use the -update flag. Data destination arguments will be re-used.\n"
    # Add a list of arguments that are accepted
    argument_output += "\nArgument flags:\n\n"
    argument_output += "-h, -help : print help menu.\n"
    argument_output += "-t [int]: set the number of threads.  Must be 1-20 default = 10.\n"
    argument_output += "-csv : write the patent data files to csv.  Setting will be saved and used on update or restart.\n"
    argument_output += "-database : write the patent data to database.  Setting will be saved on update or restart.\n"
    argument_output += "-update : check for new patent bulk data files and process them\n"
    return argument_output

# Set the config settings in file based on command arguments
def set_config_using_command_args(args_array):
    # User wants to update but but no data destination specified,
    # collect previous configuration settings
    if "update" in args_array['command_args']:
        # Check for setting data destination and write to file
        if "csv" not in args_array['command_args'] and "database" not in args_array['command_args']:
            config_settings = open(args_array['app_config_file'], "r")
            for line in config_settings.readlines():
                args_array['command_args'].append(line.strip())
            config_settings.close()

    # If command line args include data destination, then write to file
    if "csv" in args_array['command_args'] or "database" in args_array['command_args']:
        config_settings = open(args_array['app_config_file'], "w")
        for argument in args_array['command_args']:
            # Do not write update command to the file
            if argument != "update":
                config_settings.write(argument + "\n")
        config_settings.close()

    # Return the modified args_array
    return args_array

# Setup logging
def setup_logger(log_file):

    logger = logging.getLogger('USPTO_Database_Construction')
    log_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

# Handles the closing of the application
def handle_application_close(start_time, all_files_processed, args_array):

    # Close the database connection if opened
    if "database" in args_array['command_args']:
        if "database_connection" in args_array:
            args_array['database_connection'].close()

    # Print final completed message to stdout
    if all_files_processed == True:
        # Print final completed message to stdout
        print ('[All USPTO files have been processed  Time consuming:{0} Time Finished: {1}'.format(time.time()-start_time, time.strftime("%c")))
        logger.info('[All USPTO files have been processed. Time consuming:{0} Time Finished: {1}'.format(time.time()-start_time, time.strftime("%c")))
    else:
        # Print final error message to stdout
        print ('There was an error attempting to proccess the files.  Check log for details. Time consuming:{0} Time Finished: {1}'.format(time.time()-start_time, time.strftime("%c")))
        logger.info('There was an error attempting to proccess the files. Check log for details. Time consuming:{0} Time Finished: {1}'.format(time.time()-start_time, time.strftime("%c")))


# MAIN FUNCTON
# The Main function defines required variables such as filepaths, and arguments to be passed through functions
# The workflow of the main function is as follows:
# (1) Setup Logger
# (2) Parse and validate command line arguments
# (3) Collect previous configuration settings
# (4) Check for existing and if nessesary build required directory and file structure for the app
# (5) Check for existing data, look for log files and parse into workflow
# (6) Collect links if needed or look for new links if `-update` argument flag is set
# (7)


if __name__=="__main__":

    # Declare variables
    start_time=time.time()
    working_directory = os.getcwd()
    allowed_args_array = ["-csv", "-database", "-update", "-h", "-t", "-help"]
    app_default_threads = 10
    database_insert_mode = "bulk" # values include `each` and `bulk`

    # Declare filepaths
    app_temp_dirpath = working_directory + "/TMP/"
    app_csv_dirpath = working_directory + "/CSV/"
    app_log_file = working_directory + "/LOG/USPTO_app.log"
    app_config_file = working_directory + "/.USPTO_config.cnf"
    log_lock_file = working_directory + "/LOG/.logfile.lock"
    grant_process_log_file = working_directory + "/LOG/grant_links.log"
    application_process_log_file = working_directory + "/LOG/application_links.log"
    application_pair_process_log_file = working_directory + "/LOG/application_pair_links.log"
    pair_process_log_file = working_directory + "/LOG/pair_links.log"
    classification_process_log_file = working_directory + "/LOG/class_links.log"
    classification_text_filename = working_directory + "/CLS/ctaf1204.txt"
    mysql_database_reset_filename = working_directory + "/installation/uspto_create_database_mysql.sql"
    postgresql_database_reset_filename = working_directory + "/installation/uspto_create_database_postgres.sql"

    # Database args
    # database_type value should be mysql or postgresql
    database_args = {
        "database_type" : "postgresql",
        "host" : "127.0.0.1",
        "port" : 54321,
        "user" : "uspto",
        "passwd" : "Ld58KimTi06v2PnlXTFuLG4",
        "db" : "uspto",
        "charset" : "utf8"
    }

    # Used to create all required directories when application starts
    required_directory_array = [
        "/CSV/CSV_A",
        "/CSV/CSV_G",
        "/CSV/CSV_P",
        "/CLS",
        "/LOG"
        ]

    # Create an array of args that can be passed as a group
    # and appended to as needed
    args_array = {
        "working_directory" : working_directory,
        "default_threads" : app_default_threads,
        "database_type" : database_args['database_type'],
        "database_args" : database_args,
        "database_insert_mode" : database_insert_mode,
        "required_directory_array" : required_directory_array,
        "app_config_file" : app_config_file,
        "allowed_args_array" : allowed_args_array,
        "log_lock_file" : log_lock_file,
        "classification_process_log_file" : classification_process_log_file,
        "classification_text_filename" : classification_text_filename,
        "grant_process_log_file" : grant_process_log_file,
        "application_process_log_file" : application_process_log_file,
        "application_pair_process_log_file" : application_pair_process_log_file,
        "pair_process_log_file" : pair_process_log_file,
        "temp_directory" : app_temp_dirpath,
        "csv_directory" : app_csv_dirpath
    }

    # Setup logger
    setup_logger(app_log_file)
    # Include logger in the main function
    logger = logging.getLogger("USPTO_Database_Construction")

    # Perform analysis of command line args and store in args_array
    args_array["command_args"] = build_command_arguments(sys.argv, args_array)

    # If command_args are checked OK! Start app
    if args_array["command_args"]:

        # Set saved app configuration based on current command arguments
        # and collect existing config settings from file and append to args_array
        args_array = set_config_using_command_args(args_array)

        # Log start message and print to stdout
        logger.info('Starting USPTO Patent Database Builder ' + time.strftime("%c"))
        print "Starting USPTO Patent Database Builder " + time.strftime("%c")

        # Check existing app structure and create it if required
        # If true then coninue app process
        if validate_existing_file_structure(args_array):

            # Collect all links, or update with new links to log files
            build_or_update_link_files(args_array)

            # Main loop that checks if all links have been processed.
            # Read the list of files to process and eliminate the ones
            # that are marked as processed.
            all_files_processed = False
            while all_files_processed == False:

                # Read list of all required files into array from log files
                # An array is returned with list of links for each type of data to processs
                # (1) TODO UPC Classification Information Data
                # (2) TODO USC to CPC Concordence Data
                # (3) Patent Grant Biliographic Documents
                # (4) Application Bibiographic Documents
                # (5) PAIR Data

                # Collect all links by passing in log files
                # TODO: add classification parsing and PAIR link processing
                all_links_array = collect_all_links_from_file(args_array)

                #print all_links_array

                # If collecting the links array failed print error and log error
                if not all_links_array:
                    # Print the error to stdout and log
                    print 'Failed to collect links from file ' + time.strftime("%c")
                    logger.error('Failed to collect links from file ' + time.strftime("%c"))
                    # Set the main loop to exit
                    all_files_processed = "Error"

                # Else if the read list of unprocessed links is not empty
                elif len(all_links_array["grants"]) != 0 or len(all_links_array["applications"]) != 0 or len(all_links_array["classifications"]) != 0:

                    # Print message to stdout and log the number of links to be collected and parsed
                    # TODO update with classifcation data and PAIR data output
                    print str(len(all_links_array["grants"])) + " grant links will be collected. Start time: " + time.strftime("%c")
                    print str(len(all_links_array["applications"])) + " application links will be collected. Start time: " + time.strftime("%c")
                    logger.info(str(len(all_links_array["grants"])) + " grant links will be collected. Start time: " + time.strftime("%c"))
                    logger.info(str(len(all_links_array["applications"])) + " application links will be collected. Start time: " + time.strftime("%c"))

                    # Start the threading processes for the stack of links to process
                    start_thread_processes(all_links_array, args_array)

                # If both link lists are empty then all files have been processed, set main loop to exit
                else:
                    all_files_processed = True

            # Handle the closing of the application
            handle_application_close(start_time, all_files_processed, args_array)
