# USPTOExtractXML4Grant.py
# USPTO Bulk Data Parser - Extract XML 4 Grants
# Description: Imported to the main USPTOParser.py.  Extracts grant data from USPTO XML v4 files.
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

# Function used to extract data from XML4 formatted patent grants
def extract_XML4_grant(raw_data, args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Pass the url_link and format into local variables
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
                    document_id = USPTOSanitizer.fix_patent_number(document_id)[:20]
                except:
                    document_id = None
                    logger.error("No Patent Number was found for: " + url_link)
                try: kind = di.findtext('kind')[:2]
                except: kind = None
                try: pub_date = USPTOSanitizer.return_formatted_date(di.findtext('date'), args_array, document_id)
                except: pub_date = None

        # Find the main application data
        for ar in r.findall('application-reference'):
            try: app_type = ar.attrib['appl-type'][:45]
            except: app_type = None
            for di in ar.findall('document-id'):
                try: app_country = di.findtext('country')
                except: app_country = None
                try: app_no = di.findtext('doc-number')[:20]
                except: app_no = None
                try: app_date = USPTOSanitizer.return_formatted_date(di.findtext('date'), args_array, document_id)
                except: app_date = None

        # Get the series code
        try: series_code = r.findtext('us-application-series-code')[:2]
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
                    if(USPTOSanitizer.check_tag_exists(x,'section')) :
                        try: i_class_sec = x.text[:15]
                        except: i_class_sec = None
                    if(USPTOSanitizer.check_tag_exists(x,'class')) :
                        try: i_class_cls = x.text[:15]
                        except:  i_class_cls = None
                    if(USPTOSanitizer.check_tag_exists(x,'subclass')) :
                        try: i_class_sub = x.text[:15]
                        except: i_class_sub = None
                    if(USPTOSanitizer.check_tag_exists(x,'main-group')) :
                        try: i_class_mgr = x.text[:15]
                        except: i_class_mgr = None
                    if(USPTOSanitizer.check_tag_exists(x,'subgroup')) :
                        try: i_class_sgr = x.text[:15]
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
                    cpc_class_mgr = cpc_class_mgr[:15]
                    cpc_class_sgr = cpc_class_sgr[:15]
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
                n_class_main, n_subclass = USPTOSanitizer.return_class(n_class_info)
                n_class_main = n_class_main[:5]
                n_subclass = n_subclass[:15]
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
                try:
                    n_class_main, n_subclass = USPTOSanitizer.return_class(n_class_info)
                    n_class_main = n_class_main[:5]
                    n_subclass = n_subclass[:15]
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
        try: title = r.findtext('invention-title')[:500]
        except: title = None

        # Find all references cited in the grant
        for rf in r.findall('us-references-cited'):
            for rfc in rf.findall('us-citation'):
                # If the patent citation child is found must be a patent citation
                if(rfc.find('patcit') != None):
                    position = 1
                    try: citation_position = USPTOSanitizer.strip_leading_zeros(rfc.find('patcit').attrib['num'])
                    except: citation_position = position
                    for x in rfc.findall('patcit'):
                        try: citation_country = x.find('document-id').findtext('country')[:100]
                        except: citation_country = None
                        try: citation_grant_id = x.find('document-id').findtext('doc-number')[:20]
                        except: citation_grant_id = None
                        try: citation_kind = x.find('document-id').findtext('kind')[:10]
                        except: citation_kind = None
                        try: citation_name = x.find('document-id').findtext('name')[:100]
                        except: citation_name = None
                        try: citation_date = USPTOSanitizer.return_formatted_date(x.find('document-id').findtext('date'), args_array, document_id)
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
                            "Date" : citation_date,
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
                            "Date" : citation_date,
                            "Country" : citation_country,
                            "Category" : citation_category,
                            "FileName" : args_array['file_name']
                        })

                        position += 1

                # If the non patent citations are found
                elif(rfc.find('nplcit') != None):
                    position = 1
                    for x in rfc.findall('nplcit'):
                        try: citation_position = USPTOSanitizer.strip_leading_zeros(rfc.find('nplcit').attrib['num'])
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
        try:
            number_of_drawings = nof.findtext('number-of-drawing-sheets')
            number_of_drawings = number_of_drawings.split("/")[0]
        except: number_of_drawings = None
        try: number_of_figures = nof.findtext('number-of-figures')
        except: number_of_figures = None

        # Find the parties
        for prt in r.findall('us-parties'):
            # Find all applicant data
            for apts in prt.findall('us-applicants'):
                position = 1
                for apt in apts.findall('us-applicant'):
                    if(apt.find('addressbook') != None):
                        try: applicant_orgname = apt.find('addressbook').findtext('orgname')[:300]
                        except: applicant_orgname = None
                        try: applicant_first_name = apt.find('addressbook').findtext('first-name')[:100]
                        except: applicant_first_name = None
                        try: applicant_last_name = apt.find('addressbook').findtext('last-name')[:100]
                        except: applicant_last_name = None
                        try: applicant_city = apt.find('addressbook').find('address').findtext('city')[:100]
                        except: applicant_city = None
                        try: applicant_state = apt.find('addressbook').find('address').findtext('state')[:100]
                        except: applicant_state = None
                        try: applicant_country = apt.find('addressbook').find('address').findtext('country')[:100]
                        except: applicant_country = None

                        # Append SQL data into dictionary to be written later

                        processed_applicant.append({
                            "table_name" : "uspto.APPLICANT_G",
                            "GrantID" : document_id,
                            "OrgName" : applicant_orgname,
                            "Position" : position,
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
                    try: inventor_sequence = USPTOSanitizer.strip_leading_zeros(apt.attrib['sequence'])
                    except: inventor_sequence = position
                    if(apt.find('addressbook') != None):
                        try: inventor_first_name = apt.find('addressbook').findtext('first-name')[:100]
                        except: inventor_first_name = None
                        try: inventor_last_name = apt.find('addressbook').findtext('last-name')[:100]
                        except: inventor_last_name = None
                        try: inventor_city = apt.find('addressbook').find('address').findtext('city')[:100]
                        except: inventor_city = None
                        try: inventor_state = apt.find('addressbook').find('address').findtext('state')[:100]
                        except: inventor_state = None
                        try: inventor_country = apt.find('addressbook').find('address').findtext('country')[:100]
                        except: inventor_country = None
                        try: inventor_residence = apt.find('addressbook').find('address').findtext('country')[:300]
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
                    try: agent_sequence = USPTOSanitizer.strip_leading_zeros(agn.attrib['sequence'])
                    except: agent_sequence = position
                    if(agn.find('addressbook') != None):
                        try: agent_orgname = agn.find('addressbook').findtext('orgname')[:300]
                        except: agent_orgname = None
                        try: agent_last_name = agn.find('addressbook').findtext('last-name')[:100]
                        except: agent_last_name = None
                        try: agent_first_name = agn.find('addressbook').findtext('first-name')[:100]
                        except: agent_first_name = None
                        try: agent_country = agn.find('addressbook').find('address').findtext('country')[:100]
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
                    try: asn_orgname = x.find('addressbook').findtext('orgname')[:500]
                    except: asn_orgname = None
                    try: asn_role = x.find('addressbook').findtext('role')[:45]
                    except: asn_role = None
                    try: asn_city = x.find('addressbook').find('address').findtext('city')[:100]
                    except: asn_city = None
                    try: asn_state = x.find('addressbook').find('address').findtext('state')[:100]
                    except: asn_state = None
                    try: asn_country = x.find('addressbook').find('address').findtext('country')[:100]
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
            position = 1
            for x in exm.findall('primary-examiner'):
                try: exm_last_name = x.findtext('last-name')[:50]
                except: exm_last_name = None
                try: exm_first_name = x.findtext('first-name')[:50]
                except: exm_first_name = None
                try: exm_department = x.findtext('department')[:100]
                except: exm_department = None

                # Append SQL data into dictionary to be written later
                processed_examiner.append({
                    "table_name" : "uspto.EXAMINER_G",
                    "GrantID" : document_id,
                    "Position" : position,
                    "LastName" : exm_last_name,
                    "FirstName" : exm_first_name,
                    "Department" : exm_department,
                    "FileName" : args_array['file_name']
                })

                position += 1

            for x in exm.findall('assistant-examiner'):
                try: exm_last_name = x.findtext('last-name')[:50]
                except: exm_last_name = None
                try: exm_first_name = x.findtext('first-name')[:50]
                except: exm_first_name = None
                try: exm_department = x.findtext('department')[:100]
                except: exm_department = None

                # Append SQL data into dictionary to be written later
                processed_examiner.append({
                    "table_name" : "uspto.EXAMINER_G",
                    "GrantID" : document_id,
                    "Position" : position,
                    "LastName" : exm_last_name,
                    "FirstName" : exm_first_name,
                    "Department" : exm_department,
                    "FileName" : args_array['file_name']
                })

                position += 1

    # TODO: see if it's claims or description and store accordingly
    try: claims = patent_root.findtext('description')
    except: claims = None
    #print claims

    # Find the abstract
    try:
        abstract = USPTOSanitizer.return_element_text(patent_root.find('abstract'))
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
