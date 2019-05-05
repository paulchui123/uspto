# USPTOExtractXML1Application.py
# USPTO Bulk Data Parser - Extract XML1 Applications
# Description: Imported to the main USPTOParser.py.  Extracts XML v1 data for applications.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# Import Python Modules
import xml.etree.ElementTree as ET
import time
import traceback
import os
import sys

# Import USPTO Parser Functions
import USPTOLogger

# Function used to extract data from XML1 formatted patent applications
def extract_XML1_application(raw_data, args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Pass the url_link and format into local variables
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
    #print '- Starting to extract xml in USPTO application format ' + uspto_xml_format + " Start time: " + time.strftime("%c")

    #print raw_data
    # Pass the xml into Element tree object
    document_root = ET.fromstring(raw_data)
    r = document_root.find('subdoc-bibliographic-information')

    # Get and fix the document_id data
    di = r.find('document-id')
    if di is not None:
        try:
            # This document ID is NOT application number
            document_id = di.findtext('doc-number')
        except:
            document_id = None
            logger.error("No Patent Number was found for: " + url_link)
        try: kind = di.findtext('kind-code')[:2]
        except: kind = None
        try: pub_date = USPTOSanitizer.return_formatted_date(di.findtext('document-date'), args_array, document_id)
        except: pub_date = None
        try: app_type = r.findtext('publication-filing-type')[:45]
        except: app_type = None

    # Get application filing data
    ar = r.find('domestic-filing-data')
    if ar is not None:
        try:
            app_no = ar.find('application-number').findtext('doc-number')[:20]
        except: app_no = None
        try: app_date = USPTOSanitizer.return_formatted_date(ar.findtext('filing-date'), args_array, document_id)
        except: app_date = None
        try: series_code = ar.findtext('application-number-series-code')[:2]
        except: series_code = None

    technical_information_element = r.find('technical-information')
    # Init position
    position = 1
    if technical_information_element is not None:
        # Get international classification data
        ic = technical_information_element.find('classification-ipc')
        if ic is not None:

            # Process the primary international class
            icm = ic.find('classification-ipc-primary')
            #TODO: regex the class found into class, subclass and other
            #TODO: find out what maingrou and subgroup are found in this file format
            try:
                i_class_sec, i_class, i_subclass, i_class_mgr, i_class_sgr = USPTOSanitizer.return_international_class(icm.findtext('ipc'))
                i_class_sec = i_class_sec[:15]
                i_class = i_class[:15]
                i_subclass = i_subclass[:15]
                i_class_mgr = i_class_mgr[:15]
                i_class_sgr = i_class_sgr[:15]
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

            # Process any secondary international classes
            ics = ic.findall('classification-ipc-secondary')
            if ics is not None:
                for ics_item in ics:
                    try:
                        i_class_sec, i_class, i_subclass, i_class_mgr, i_class_sgr = USPTOSanitizer.return_international_class(ics_item.findtext('ipc'))
                        i_class_sec = i_class_sec[:15]
                        i_class = i_class[:15]
                        i_subclass = i_subclass[:15]
                        i_class_mgr = i_class_mgr[:15]
                        i_class_sgr = i_class_sgr[:15]
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
    # init position
    position = 1
    if nc is not None:

        uspc = nc.find('classification-us-primary').find('uspc')
        try: n_class_main = uspc.findtext('class')[:5]
        except: n_class_main = None
        try: n_subclass = uspc.findtext('subclass')[:15]
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

        us_classification_secondary_element = nc.find('classification-us-secondary')
        if us_classification_secondary_element is not None:
            uspc = us_classification_secondary_element.find('uspc')
            try: n_class_main = uspc.findtext('class')[:5]
            except: n_class_main = None
            try: n_subclass = uspc.findtext('subclass')[:5]
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
    try: title = technical_information_element.findtext('title-of-invention')[:500]
    except: title = None

    # Get inventor data
    iv = r.find('inventors')
    if iv is not None:

        # Init position
        position = 1

        for inventor in iv.findall('first-named-inventor'):
            n = inventor.find('name')
            try: inventor_first_name = n.findtext('given-name')[:100]
            except: inventor_first_name = None
            try: inventor_last_name = n.findtext('family-name')[:100]
            except: inventor_last_name = None

            res = inventor.find('residence')
            if res is not None:
                residence_us = res.find('residence-us')
                if residence_us is not None:
                    try: inventor_city = residence_us.findtext('city')[:100]
                    except: inventor_city = None
                    try: inventor_state = residence_us.findtext('state')[:100]
                    except: inventor_state = None
                    try: inventor_country = residence_us.findtext('country-code')[:100]
                    except: inventor_country = None
                residence_non_us = res.find('residence-non-us')
                if residence_non_us is not None:
                    try: inventor_city = residence_non_us.findtext('city')[:100]
                    except: inventor_city = None
                    try: inventor_state = residence_non_us.findtext('state')[:100]
                    except: inventor_state = None
                    try: inventor_country = residence_non_us.findtext('country-code')[:100]
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
            if inventor is not None:
                n = inventor.find('name')
                if n is not None:
                    try: inventor_first_name = n.findtext('given-name')[:100]
                    except: inventor_first_name = None
                    try: inventor_last_name = n.findtext('family-name')[:100]
                    except: inventor_last_name = None

                res = inventor.find('residence')
                if res is not None:
                    residence_us = res.find('residence-us')
                    if residence_us is not None:
                        try: inventor_city = residence_us.findtext('city')[:100]
                        except: inventor_city = None
                        try: inventor_state = residence_us.findtext('state')[:100]
                        except: inventor_state = None
                        try: inventor_country = residence_us.findtext('country-code')[:100]
                        except: inventor_country = None
                    residence_non_us = res.find('residence-non-us')
                    if residence_non_us is not None:
                        try: inventor_city = residence_non_us.findtext('city')[:100]
                        except: inventor_city = None
                        try: inventor_state = residence_non_us.findtext('state')[:100]
                        except: inventor_state = None
                        try: inventor_country = residence_non_us.findtext('country-code')[:100]
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

    assignee_element = r.find('assignee')
    if assignee_element is not None:
        # init position
        position = 1

        try: asn_role = assignee_element.findtext('assignee-type')[:100]
        except: asn_role = None
        on = assignee_element.find('organization-name')
        try: asn_orgname = USPTOSanitizer.return_element_text(on)[:300]
        except: asn_orgname = None
        ad = assignee_element.find('address')
        try: asn_city = ad.findtext('city')[:100]
        except: asn_city = None
        try: asn_state = ad.findtext('state')[:100]
        except: asn_state = None
        try: asn_country = ad.find('country').findtext('country-code')[:100]
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
        try:
            agent_orgname += agent_element.findtext('name-2')
            agent_orgname = agent_orgname[:300]
        except: agent_orgname = None
        try:
            adresss_element = agent_element.find('address')
            if address_element is not None:
                try: agent_city = adresss_element.findtext('city')[:100]
                except: agent_city = None
                try: agent_state = adresss_element.findtext('state')[:100]
                except: agent_state = None
                try: agent_country = adresss_element.find('country').findtext('country-code')[:100]
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
    try: abstract = USPTOSanitizer.return_element_text(document_root.find('subdoc-abstract')).replace("\n", " ").strip()
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
