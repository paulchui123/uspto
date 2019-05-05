# USPTOSanitizer.py
# USPTO Bulk Data Parser - Sanitizer
# Description: Imported to the main USPTOParser.py.  Contains data sanitization functions.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# Import Python Modules
import xml.etree.ElementTree as ET
import logging
import time
import os
import sys
import re
from HTMLParser import HTMLParser
#from htmlentitydefs import name2codepoint
import string
import traceback

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
    #print i_class_sgr

    # Return array of data
    return [i_class_sec, i_class, i_subclass, i_class_mgr, i_class_sgr]

# returns the cpc class breakdown by class and subclass
def return_cpc_class(class_string):
    cpc_class_sec = class_string[0]
    class_string = class_string[1:len(class_string)]

# Function to accept the date and return in MYSQL formated date
# TODO: fix the date parsing. Problem of possibly one or two characters for month and day
def return_formatted_date(time_str, args_array, document_id):

    # Import the logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Check if None has been passed in
    if time_str is None:
        logger.warning("None Type object was found as date for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
        return None
    # Check if '0000-01-01' has been passed in
    elif time_str == '0000-01-01' or time_str == "00000101":
        logger.warning("'0000-01-01' was found as date for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
        return None
    # Check if '0000-00-00' has been passed in
    elif time_str == '0000-00-00' or time_str == "00000000":
        logger.warning("'0000-00-00' was found as date for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
        return None

    # Check all other conditions based on string length
    else:
        # If the string length is correct
        if len(time_str) ==  8:

            # If the year value is out of range
            if time_str[0:4] == "0000":
                logger.warning("'0000' was found as year for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
                return None
            # Else set the year value
            else: year = time_str[0:4]

            # If the month value is out of range
            if time_str[4:6] == "00" : month = "01"
            elif int(time_str[4:6]) > 12 or int(time_str[4:6]) < 1: month = "01"
            else: month = time_str[4:6]

            # If the day value is out of range
            if time_str[6:8] == "00" : day = "01"
            elif int(time_str[6:8]) > 31 or int(time_str[6:8]) < 1 : day = "01"
            else: day = time_str[6:8]

            # Validate the date for other erors such as leap year, etc.
            try:
                # Validate the date
                datetime.datetime(year, month, day)
                # Finally return the fixed time string
                return year + '-' + month + '-' + day
            except ValueError:
                logger.warning("Could not validate date: " + time_str +  " for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
                return False

            # Finally return the fixed time string
            return year + '-' + month + '-' + day

        # If the string length is too long
        elif len(time_str) == 9:
            #print "Date length == 9"
            time_str = time_str.replace("\n", "").replace("\r", "")
            if len(time_str) == 9:
                # Log that a bad date was found and could not be cleaned
                logger.warning("Malformed date was found on length == 9 string: " + time_str + " for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
            if  time_str[0:4] == "0000":
                 return None
            else:
                if  time_str[4:6] == "00" : month = "01"
                else: month = time_str[4:6]
                if time_str[6:8] == "00" : day = "01"
                else: day = time_str[6:8]
                return time_str[0:4] + '-' + month + '-' + day
        else:
            # Log that a bad date was found and could not be cleaned
            logger.warning("Malformed date was found on length != 8 or 9 string: " + time_str + " for " + args_array['document_type'] + " documentID: " + document_id + " in the link: " + args_array['url_link'])
            return None

# Used to fix patent numbers
def return_patent_number(patternStr,inputStr):
    c = re.compile(patternStr)
    r = c.match(inputStr)
    return r

# Strip leading zero and '&' from patent numbers
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

# Strips tags from XMLTree element
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
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

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

        # Remove all non ASCII characters
        line = line.decode("ascii", "ignore")

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

        # Remove all non ASCII characters
        line = line.decode("ascii", "ignore")

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
