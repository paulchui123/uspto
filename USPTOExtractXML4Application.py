# USPTOExtractXML4Application.py
# USPTO Bulk Data Parser - Extract XML4 Applications
# Description: Imported to the main USPTOParser.py.  Extracts XML v4 data for applications.
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
import USPTOSanitizer

# Function used to extract data from XML4 formatted patent applications
def extract_XML4_application(raw_data, args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Pass the url_link and format into local variables
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
            document_id = USPTOSanitizer.fix_patent_number(document_id)
        except:
            document_id = None
            logger.error("No Patent Number was found for: " + url_link)
        try: kind = pub_doc.findtext('kind')[:2]
        except: kind = None
        try: pub_date = USPTOSanitizer.return_formatted_date(pub_doc.findtext('date'), args_array, document_id)
        except: pub_date = None

        # Get application reference data
        ar = r.find('application-reference')
        if ar is not None:
            try: app_type = ar.attrib['appl-type'][:45]
            except: app_type = None
            app_doc = ar.find('document-id')
            try: app_country = app_doc.findtext('country')
            except: app_country = None
            try: app_no = app_doc.findtext('doc-number')[:20]
            except: app_no = None
            try: app_date = USPTOSanitizer.return_formatted_date(app_doc.findtext('date'), args_array, document_id)
            except: app_date = None
            # Get series code
            try: series_code = r.findtext('us-application-series-code')[:2]
            except: series_code = None

        # Get priority Claims
        pcs = r.find('priority-claims')
        if pcs is not None:
            for pc in pcs.findall('priority-claim'):
                try: pc_sequence = USPTOSanitizer.strip_leading_zeros(pc.attrib['sequence'])
                except: pc_sequence = None
                try: pc_kind = pc.attrib['kind'][:100]
                except: pc_kind = None
                try: pc_country = pc.findtext('country')[:100]
                except: pc_country = None
                try: pc_doc_num = pc.findtext('doc-number')[:100]
                except: pc_doc_num = None
                try: pc_date = USPTOSanitizer.return_formatted_date(pc.findtext('date'), args_array, document_id)
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
                    if(USPTOSanitizer.check_tag_exists(x,'section')): i_class_sec = x.text[:100]
                    if(USPTOSanitizer.check_tag_exists(x,'class')): i_class = x.text[:15]
                    if(USPTOSanitizer.check_tag_exists(x,'subclass')): i_subclass = x.text[:15]
                    if(USPTOSanitizer.check_tag_exists(x,'main-group')): i_class_mgr = x.text[:15]
                    if(USPTOSanitizer.check_tag_exists(x,'subgroup')): i_class_sgr = x.text[:15]

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
            try:
                n_class_main, n_subclass = USPTOSanitizer.return_class(n_class_info)
                n_class_main = n_class_main[:5]
                n_subclass = n_subclass[:15]
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
                    try:
                        n_class_main, n_subclass = USPTOSanitizer.return_class(n_class_info)
                        n_class_main = n_class_main[:5]
                        n_subclass = n_subclass[:15]
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
                    try: cpc_section = cpc_class_item.findtext('section')[:15]
                    except: cpc_section = None
                    try: cpc_class = cpc_class_item.findtext('class')[:15]
                    except: cpc_class = None
                    try: cpc_subclass = cpc_class_item.findtext('subclass')[:15]
                    except: cpc_subclass = None
                    try: cpc_mgr = cpc_class_item.findtext('main-group')[:15]
                    except: cpc_mgr = None
                    try: cpc_sgr = cpc_class_item.findtext('subgroup')[:15]
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
                    try: cpc_section = cpc_class_item.findtext('section')[:15]
                    except: cpc_section = None
                    try: cpc_class = cpc_class_item.findtext('class')[:15]
                    except: cpc_class = None
                    try: cpc_subclass = cpc_class_item.findtext('subclass')[:15]
                    except: cpc_subclass = None
                    try: cpc_mgr = cpc_class_item.findtext('main-group')[:15]
                    except: cpc_mgr = None
                    try: cpc_sgr = cpc_class_item.findtext('subgroup')[:15]
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
            title = r.findtext('invention-title')[:500]
        except:
            title = None
            logger.error("Title not Found for :" + url_link + " Application ID: " + app_no)

        # Get number of claims
        try: claims_num = r.findtext('number-of-claims')
        except: claims_num = None

        # Get number of figure, drawings
        nof = r.find('figures')
        if nof is not None:
            try: number_of_drawings = nof.findtext('number-of-drawing-sheets')
            except: number_of_drawings = None
            try: number_of_figures = nof.findtext('number-of-figures')
            except: number_of_figures = None
        else:
            number_of_drawings = None
            number_of_figures = None

        # Increment position
        position = 1
        # Get Associated party data
        parties_element = r.find('us-parties')
        if parties_element is not None:
            applicant_element = parties_element.find('us-applicants')
            # Get Applicant data
            for applicant_item in applicant_element.findall('us-applicant'):
                if(applicant_item.find('addressbook') != None):
                    try: applicant_orgname = applicant_item.find('addressbook').findtext('orgname')[:300]
                    except: applicant_orgname = None
                    try: applicant_role = applicant_item.find('addressbook').findtext('role')
                    except: applicant_role = None
                    try: applicant_city = applicant_item.find('addressbook').find('address').findtext('city')[:100]
                    except: applicant_city = None
                    try: applicant_state = applicant_item.find('addressbook').find('address').findtext('state')[:100]
                    except: applicant_state = None
                    try: applicant_country = applicant_item.find('addressbook').find('address').findtext('country')[:100]
                    except: applicant_country = None
                    try: applicant_first_name = applicant_item.find('addressbook').findtext('first-name')[:100]
                    except: applicant_first_name = None
                    try: applicant_last_name = applicant_item.find('addressbook').findtext('last-name')[:100]
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
            if invs is not None:
                # Get all inventors
                for inv in invs.findall("inventor"):
                    if(inv.find('addressbook') != None):
                        try: inventor_first_name = inv.find('addressbook').findtext('first-name')[:100]
                        except: inventor_first_name = None
                        try: inventor_last_name = inv.find('addressbook').findtext('last-name')[:100]
                        except: inventor_last_name = None
                        try: inventor_city = inv.find('addressbook').find('address').findtext('city')[:100]
                        except: inventor_city = None
                        try: inventor_state = inv.find('addressbook').find('address').findtext('state')[:100]
                        except: inventor_state = None
                        try: inventor_country = inv.find('addressbook').find('address').findtext('country')[:100]
                        except: inventor_country = None
                        try: inventor_nationality = inv.find('nationality').findtext('country')[:100]
                        except: inventor_nationality = None
                        try: inventor_residence = inv.find('residence').findtext('country')[:300]
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
                        try: atn_orgname = agent_item.find('addressbook').findtext('orgname')[:300]
                        except: atn_orgname = None
                        try: atn_last_name = agent_item.find('addressbook').findtext('last-name')[:100]
                        except: atn_last_name = None
                        try: atn_first_name = agent_item.find('addressbook').findtext('first-name')[:100]
                        except: atn_first_name = None
                        try: atn_country = agent_item.find('addressbook').find('address').findtext('country')[:100]
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
        assignee_element = r.find('assignees')
        # Init position
        position += 1
        if assignee_element is not None:
            for assignee_item in assignee_element.findall('assignee'):
                if(assignee_item.find('addressbook') != None):
                    try: assignee_orgname = assignee_item.find('addressbook').findtext('orgname')[:300]
                    except: assignee_orgname = None
                    try: assignee_role = assignee_item.find('addressbook').findtext('role')[:45]
                    except: assignee_role = None
                    try: assignee_city = assignee_item.find('addressbook').find('address').findtext('city')[:100]
                    except: assignee_city = None
                    try: assignee_state = assignee_item.find('addressbook').find('address').findtext('state')[:100]
                    except: assignee_state = None
                    try: assignee_country = assignee_item.find('addressbook').find('address').findtext('country')[:100]
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
        if abstract_element is not None:
            abstract = USPTOSanitizer.return_element_text(abstract_element)
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
