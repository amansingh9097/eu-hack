import pandas as pd
import os
import xml.dom.minidom as minidom
import tarfile
import shutil

# parser = argparse.ArgumentParser(description="pass directory containing all the xml files.")
# parser.add_argument("-i", dest="inputfile", help="complete path of the input directory containing xml files", required=True)
# parser.add_argument("-o", dest="outputfile", help="name of CSV file to save as")
# args = parser.parse_args()


def parseXML(xml_file):
    """
    input: .xml file
    output: dataframe with each xml file parsed in a row
    """
    df = pd.DataFrame()
    
    # FILE NAME
    df.loc[0, 'FILE NAME'] = xml_file.split('/')[-1]
    
    doc = minidom.parse(xml_file)
    
    # LANG
    lang_list = doc.childNodes[0].getElementsByTagName('FORM_LG_LIST')[0].childNodes[0].wholeText.strip().split(' ')
    if 'EN' in lang_list:
        EN_POS = lang_list.index('EN')
        df.loc[0, 'LANG'] = 'EN'
    else:
        EN_POS = 0
    
    # DOC ID
    df.loc[0, 'DOC ID'] = doc.firstChild.getAttribute('DOC_ID')
    
    # DOC VERSION
    if doc.firstChild.getAttribute('VERSION'):
        df.loc[0, 'DOC VERSION'] = doc.firstChild.getAttribute('VERSION')
    else:
        if doc.firstChild.getElementsByTagName('OTH_NOT'):
            df.loc[0, 'DOC VERSION'] = doc.firstChild.getElementsByTagName('OTH_NOT')[0].getAttribute('VERSION')
        else:
            if doc.firstChild.getElementsByTagName('CONTRACT'):
                df.loc[0, 'DOC VERSION'] = doc.firstChild.getElementsByTagName('CONTRACT')[0].getAttribute('VERSION')
    
    # URL
    df.loc[0, 'URL'] = "https://ted.europa.eu/udl?uri=TED:NOTICE:"+doc.firstChild.getAttribute('DOC_ID')+":TEXT:EN:HTML"
    
    # DOC TITLE
    for item in doc.firstChild.getElementsByTagName('ML_TI_DOC'):
        if item.getAttribute('LG') == 'EN':
            title = ""
            for child in item.childNodes:
                if child.tagName == "TI_CY":
                    title += child.childNodes[0].wholeText + "-"
                if child.tagName == "TI_TOWN":
                    title += child.childNodes[0].wholeText + ": "
                if child.tagName == "TI_TEXT":
                    if child.childNodes[0].childNodes:
                        title += child.childNodes[0].childNodes[0].wholeText
                    else:
                        title += child.childNodes[0].wholeText
    df.loc[0, 'DOC TITLE'] = title
    
    # DOC REF
    if doc.firstChild.getElementsByTagName('NO_DOC_OJS')[0].firstChild != None:
        df.loc[0, 'DOC REF'] = doc.firstChild.getElementsByTagName('NO_DOC_OJS')[0].firstChild.wholeText
    
    # DOC REGION
    if doc.firstChild.getElementsByTagName('STI_DOC'):
        if doc.firstChild.getElementsByTagName('STI_DOC')[EN_POS].childNodes[0].childNodes[0] != None:
            df.loc[0, 'DOC REGION'] = doc.firstChild.getElementsByTagName('STI_DOC')[EN_POS].childNodes[0].childNodes[0].nodeValue
    
    # CONTRACT NATURE
    if doc.firstChild.getElementsByTagName('NC_CONTRACT_NATURE'):
        df.loc[0, 'CONTRACT NATURE'] = doc.firstChild.getElementsByTagName('NC_CONTRACT_NATURE')[0].firstChild.nodeValue
    else:
        if doc.firstChild.getElementsByTagName('OTH_NOT'):
            for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                if item.getAttribute('LG') == 'EN':
                    TI_MARK = item.getElementsByTagName('TI_MARK')
                    TXT_MARK = item.getElementsByTagName('TXT_MARK')
                    NO_MARK = item.getElementsByTagName('NO_MARK')
                    for i in range(len(item.getElementsByTagName('NO_MARK'))):
                        if TI_MARK[i].firstChild:
                            if TI_MARK[i].firstChild.nodeValue.lower() == 'nature of contract':
                                childs_txt = []
                                for child in TXT_MARK[i].childNodes: # txt
                                    for kid in child.childNodes:
                                        if kid.nodeValue != None:
                                            childs_txt.append(kid.nodeValue)
                                childs_txt = [str(n) if n is None else n for n in childs_txt]
                                df.loc[0, 'CONTRACT NATURE'] = ', '.join(childs_txt)
    
    # DOC TYPE
    if doc.firstChild.getElementsByTagName('TD_DOCUMENT_TYPE')[0].firstChild != None:
        df.loc[0, 'DOC TYPE'] = doc.firstChild.getElementsByTagName('TD_DOCUMENT_TYPE')[0].firstChild.wholeText
    
    # DOC PUBLICATION DATE
    if doc.firstChild.getElementsByTagName('DATE_PUB')[0].firstChild != None:
        df.loc[0, 'DOC PUBLICATION DATE'] = doc.firstChild.getElementsByTagName('DATE_PUB')[0].firstChild.wholeText
    
    # CONTRACTING AUTHORITY
    if doc.firstChild.getElementsByTagName('ML_AA_NAMES'):
        lang_attributes = []
        for child in doc.firstChild.getElementsByTagName('ML_AA_NAMES')[0].childNodes:
            lang_attributes.append(child.getAttribute('LG'))

        if 'EN' in lang_attributes:
            df.loc[0, 'CONTRACTING AUTHORITY'] = doc.firstChild.getElementsByTagName('ML_AA_NAMES')[0].childNodes[EN_POS].childNodes[0].nodeValue
        else:
            df.loc[0, 'CONTRACTING AUTHORITY'] = doc.firstChild.getElementsByTagName('ML_AA_NAMES')[0].childNodes[0].childNodes[0].nodeValue
    else:
        if doc.firstChild.getElementsByTagName('OTH_NOT'):
            for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                if item.getAttribute('LG') == 'EN':
                    TI_MARK = item.getElementsByTagName('TI_MARK')
                    TXT_MARK = item.getElementsByTagName('TXT_MARK')
                    NO_MARK = item.getElementsByTagName('NO_MARK')
                    for i in range(len(item.getElementsByTagName('NO_MARK'))):
                        if TI_MARK[i].firstChild:
                            if TI_MARK[i].firstChild.nodeValue.lower() == 'contracting authority':
                                childs_txt = []
                                for child in TXT_MARK[i].childNodes: # txt
                                    for kid in child.childNodes:
                                        if kid.nodeValue != None:
                                            childs_txt.append(kid.nodeValue)
                                childs_txt = [str(n) if n is None else n for n in childs_txt]
                                df.loc[0, 'CONTRACTING AUTHORITY'] = ','.join(childs_txt)

    # POSTAL ADDRESS
    if doc.firstChild.getElementsByTagName('ADDRESS'):
        if doc.firstChild.getElementsByTagName('ADDRESS')[0].firstChild != None:
            df.loc[0, 'POSTAL ADDRESS'] = doc.firstChild.getElementsByTagName('ADDRESS')[0].firstChild.wholeText

    # TOWN
    if doc.firstChild.getElementsByTagName('TOWN'):
        if doc.firstChild.getElementsByTagName('TOWN')[0].firstChild != None:
            df.loc[0, 'TOWN'] = doc.firstChild.getElementsByTagName('TOWN')[0].firstChild.wholeText

    # POSTAL CODE
    if doc.firstChild.getElementsByTagName('POSTAL_CODE'):
        if doc.firstChild.getElementsByTagName('POSTAL_CODE')[0].firstChild != None:
            df.loc[0, 'POSTAL CODE'] = doc.firstChild.getElementsByTagName('POSTAL_CODE')[0].firstChild.wholeText

    # COUNTRY CODE
    if doc.firstChild.getElementsByTagName('COUNTRY'):
        df.loc[0, 'COUNTRY CODE'] = doc.firstChild.getElementsByTagName('COUNTRY')[0].getAttribute('VALUE')

    # COUNTRY
    for element in doc.firstChild.getElementsByTagName('ML_TI_DOC'):
        if element.getAttribute('LG') == 'EN':
            df.loc[0, 'COUNTRY'] = element.getElementsByTagName('TI_CY')[0].firstChild.nodeValue

    # ORIGINAL NUTS CODE
    if doc.firstChild.getElementsByTagName('ORIGINAL_NUTS'):
        df.loc[0, 'ORIGINAL NUTS CODE'] = doc.firstChild.getElementsByTagName('ORIGINAL_NUTS')[0].getAttribute('CODE')

    # ORIGINAL NUTS LOCATION
    if doc.firstChild.getElementsByTagName('ORIGINAL_NUTS'):
        if doc.firstChild.getElementsByTagName('ORIGINAL_NUTS')[0].firstChild != None:
            df.loc[0, 'ORIGINAL NUTS LOCATION'] = doc.firstChild.getElementsByTagName('ORIGINAL_NUTS')[0].firstChild.wholeText

    # NUTS CODE
    if doc.firstChild.getElementsByTagName('n2016:PERFORMANCE_NUTS'):
        df.loc[0, 'NUTS CODE'] = doc.firstChild.getElementsByTagName('n2016:PERFORMANCE_NUTS')[0].getAttribute('CODE')

    # NUTS LOCATION
    if doc.firstChild.getElementsByTagName('n2016:PERFORMANCE_NUTS'):
        if doc.firstChild.getElementsByTagName('n2016:PERFORMANCE_NUTS')[0].firstChild != None:
            df.loc[0, 'NUTS LOCATION'] = doc.firstChild.getElementsByTagName('n2016:PERFORMANCE_NUTS')[0].firstChild.wholeText

    # CONTRACTING AUTHORITY TYPE
    if doc.firstChild.getElementsByTagName('AA_AUTHORITY_TYPE'): 
        if doc.firstChild.getElementsByTagName('AA_AUTHORITY_TYPE')[0].firstChild != None:
            df.loc[0, 'CONTRA TING AUTHORITY TYPE'] = doc.firstChild.getElementsByTagName('AA_AUTHORITY_TYPE')[0].firstChild.nodeValue

    # MAIN ACTIVITY
    if doc.firstChild.getElementsByTagName('CA_ACTIVITY'):
        df.loc[0, 'MAIN ACTIVITY'] = doc.firstChild.getElementsByTagName('CA_ACTIVITY')[EN_POS].getAttribute('VALUE')
    else:
        if doc.firstChild.getElementsByTagName('MA_MAIN_ACTIVITIES'): 
            if doc.firstChild.getElementsByTagName('MA_MAIN_ACTIVITIES')[0].firstChild != None:
                df.loc[0, 'MAIN ACTIVITY'] = doc.firstChild.getElementsByTagName('MA_MAIN_ACTIVITIES')[0].firstChild.nodeValue

    # OTHER ACTIVITY
    if doc.firstChild.getElementsByTagName('CA_ACTIVITY_OTHER'):
        if doc.firstChild.getElementsByTagName('CA_ACTIVITY_OTHER')[EN_POS].hasAttribute('VALUE'):
            df.loc[0, 'OTHER ACTIVITY'] = doc.firstChild.getElementsByTagName('CA_ACTIVITY_OTHER')[EN_POS].getAttribute('VALUE')
        else:
            if doc.firstChild.getElementsByTagName('CA_ACTIVITY_OTHER')[EN_POS].firstChild != None:
                df.loc[0, 'OTHER ACTIVITY'] = doc.firstChild.getElementsByTagName('CA_ACTIVITY_OTHER')[EN_POS].firstChild.nodeValue

    # TITLE
    if doc.firstChild.getElementsByTagName('TITLE'): 
        if doc.firstChild.getElementsByTagName('TITLE')[EN_POS].firstChild.firstChild != None:
            df.loc[0, 'TITLE'] = doc.firstChild.getElementsByTagName('TITLE')[EN_POS].firstChild.firstChild.nodeValue
    else:
        if doc.firstChild.getElementsByTagName('TITLE_CONTRACT'): 
            if doc.firstChild.getElementsByTagName('TITLE_CONTRACT')[EN_POS].firstChild.firstChild != None:
                df.loc[0, 'TITLE'] = doc.firstChild.getElementsByTagName('TITLE_CONTRACT')[EN_POS].firstChild.firstChild.nodeValue
        else:
            if doc.firstChild.getElementsByTagName('OTH_NOT'):
                for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                    if item.getAttribute('LG') == 'EN':
                        TI_MARK = item.getElementsByTagName('TI_MARK')
                        TXT_MARK = item.getElementsByTagName('TXT_MARK')
                        NO_MARK = item.getElementsByTagName('NO_MARK')
                        for i in range(len(item.getElementsByTagName('TI_MARK'))):
                            if TI_MARK[i].firstChild:
                                if TI_MARK[i].firstChild.nodeValue.lower() == 'programme title':
                                    childs_txt = []
                                    for child in TXT_MARK[i].childNodes: # txt
                                        for kid in child.childNodes:
                                            if kid.nodeValue != None:
                                                childs_txt.append(kid.nodeValue)
                                    childs_txt = [str(n) if n is None else n for n in childs_txt]
                                    df.loc[0, 'TITLE'] = ', '.join(childs_txt)
            

    # CONTRACT REF NUM
    if doc.firstChild.getElementsByTagName('REFERENCE_NUMBER'): 
        if doc.firstChild.getElementsByTagName('REFERENCE_NUMBER')[EN_POS].firstChild != None:
            df.loc[0, 'CONTRACT REF NUM'] = doc.firstChild.getElementsByTagName('REFERENCE_NUMBER')[EN_POS].firstChild.nodeValue

    # ORIGINAL CPV CODE & DESCR
    if doc.firstChild.getElementsByTagName('ORIGINAL_CPV'):
        df.loc[0, 'ORIGINAL CPV CODE'] = doc.firstChild.getElementsByTagName('ORIGINAL_CPV')[0].getAttribute('CODE')
        df.loc[0, 'ORIGINAL CPV DESCR'] = doc.firstChild.getElementsByTagName('ORIGINAL_CPV')[0].firstChild.nodeValue

    # MAIN CPV CODE
    if doc.firstChild.getElementsByTagName('CPV_CODE'):
        df.loc[0, 'MAIN CPV CODE'] = doc.firstChild.getElementsByTagName('CPV_CODE')[EN_POS].getAttribute('CODE')

    # CONTRACT TYPE
    if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT'):
        for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT')[EN_POS].childNodes:
            if obj.tagName == 'TYPE_CONTRACT':
                df.loc[0, 'CONTRACT TYPE'] = obj.getAttribute('CTYPE')
    
    # CONTRACT DESCR
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'contract description':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes:
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'CONTRACT DESCR'] = ', '.join(childs_txt)
            
    # CONTRACT SHORT DESCR
    if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT'):
        for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT')[EN_POS].childNodes:
            if obj.tagName == 'SHORT_DESCR':
                short_descr = []
                for para in obj.childNodes:
                    for p in para.childNodes:
                        if p.nodeValue != None:
                            short_descr.append(p.nodeValue)
                df.loc[0, 'CONTRACT SHORT DESCR'] = ','.join(short_descr)
    else:
        if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT_INFORMATION'):
            for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT_INFORMATION')[EN_POS].childNodes:
                for element in obj.childNodes:
                    if element.tagName == 'SHORT_CONTRACT_DESCRIPTION': 
                        if element.firstChild.firstChild != None:
                            df.loc[0, 'CONTRACT SHORT DESCR'] = element.firstChild.firstChild.nodeValue

    # LOT DIVISION
    if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT'):
        for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT')[EN_POS].childNodes:
            if obj.tagName == 'LOT_DIVISION':
                df.loc[0, 'LOT DIVISION'] = obj
            else:
                if obj.tagName == 'NO_LOT_DIVISION':
                    df.loc[0, 'LOT DIVISION'] = obj.tagName
    else:
        if doc.firstChild.getElementsByTagName('LOT_DIVISION'):
            lots = element.getElementsByTagName('LOT_DIVISION')[0]
            if lots.childNodes:
                df.loc[0, 'LOT DIVISION'] = "".join([ele.childNodes[0].wholeText for ele in lots.childNodes[0].getElementsByTagName('P')])
            else:
                lot_description = []
                while lots.nextSibling.tagName == 'OBJECT_DESCR':
                    tags = lots.nextSibling
                    for titles in lots.getElementsByTagName('TITLE'):
                        lot_description.append(titles.childNodes[0].childNodes[0].wholeText)
                    df.loc[0, 'LOT DIVISION'] = "".join(lot_description)
        else:
            if doc.firstChild.getElementsByTagName('OTH_NOT'):
                for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                    if item.getAttribute('LG') == 'EN':
                        TI_MARK = item.getElementsByTagName('TI_MARK')
                        TXT_MARK = item.getElementsByTagName('TXT_MARK')
                        NO_MARK = item.getElementsByTagName('NO_MARK')
                        for i in range(len(item.getElementsByTagName('NO_MARK'))):
                            if TI_MARK[i].firstChild:
                                if (TI_MARK[i].firstChild.nodeValue.lower() == 'number and titles of lots') | (TI_MARK[i].firstChild.nodeValue.lower() == 'lot number and lot title'):
                                    childs_txt = []
                                    for child in TXT_MARK[i].childNodes:
                                        for kid in child.childNodes:
                                            if kid.nodeValue != None:
                                                childs_txt.append(kid.nodeValue)
                                    childs_txt = [str(n) if n is None else n for n in childs_txt]
                                    df.loc[0, 'LOT DIVISION'] = ', '.join(childs_txt)

    # CONTRACT PROCUREMENT DESCR
    if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT'):
        for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT')[EN_POS].childNodes:
            if obj.tagName == 'OBJECT_DESCR':
                for item in obj.childNodes:
                    if item.tagName == 'SHORT_DESCR':
                        pro_short_descr = []
                        for para in item.childNodes:
                            for p in para.childNodes:
                                if p.nodeValue != None:
                                    pro_short_descr.append(p.nodeValue)
                        df.loc[0, 'CONTRACT PROCUREMENT DESCR'] = ','.join(pro_short_descr)
                        

    # ADDITIONAL CPV CODES
    if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT'):
        for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT')[EN_POS].childNodes:
            if obj.tagName == 'OBJECT_DESCR':
                cpv_add_code = []
                for item in obj.childNodes:
                    if item.tagName == 'CPV_ADDITIONAL':
                        for child in item.childNodes:
                            cpv_add_code.append(child.getAttribute('CODE'))
                
                df.loc[0, 'ADDITIONAL CPV CODES'] = ', '.join(cpv_add_code)

    # VALUE, VALUE TYPE, VALUE CURRENCY
    if doc.firstChild.getElementsByTagName('VALUES'):
        if doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE'):
            df.loc[0, 'VALUE TYPE'] = doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE')[0].getAttribute('TYPE')
            df.loc[0, 'VALUE'] = doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE')[0].firstChild.nodeValue
            df.loc[0, 'VALUE CURRENCY'] = doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE')[0].getAttribute('CURRENCY')
        elif doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE_RANGE'):
            df.loc[0, 'VALUE TYPE'] = doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE_RANGE')[0].getAttribute('TYPE')
            df.loc[0, 'VALUE'] = doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE_RANGE')[0].getElementsByTagName('LOW')[0].firstChild.nodeValue + \
            ' to ' + doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE_RANGE')[0].getElementsByTagName('HIGH')[0].firstChild.nodeValue
            df.loc[0, 'VALUE CURRENCY'] = doc.firstChild.getElementsByTagName('VALUES')[0].getElementsByTagName('VALUE_RANGE')[0].getAttribute('CURRENCY')
            
    # EST TOTAL VALUE
    if doc.firstChild.getElementsByTagName('VAL_ESTIMATED_TOTAL'): 
        if doc.firstChild.getElementsByTagName('VAL_ESTIMATED_TOTAL')[0].firstChild != None:
            df.loc[0, 'EST TOTAL VALUE'] = doc.firstChild.getElementsByTagName('VAL_ESTIMATED_TOTAL')[0].firstChild.nodeValue + ' ' \
            + doc.firstChild.getElementsByTagName('VAL_ESTIMATED_TOTAL')[0].getAttribute('CURRENCY')

    # CONTRACTOR
    if doc.firstChild.getElementsByTagName('CONTRACTORS'): 
        if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('OFFICIALNAME')[0].firstChild != None:
            df.loc[0, 'CONTRACTORS'] = doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('OFFICIALNAME')[0].firstChild.nodeValue

    # CONTRACTOR ADDRESS DETAILS
    if doc.firstChild.getElementsByTagName('CONTRACTORS'):
        details = []
        if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('ADDRESS'):
            if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('ADDRESS')[0].firstChild.nodeValue != None:
                details.append(doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('ADDRESS')[0].firstChild.nodeValue)
        if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('TOWN'):
            if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('TOWN')[0].firstChild.nodeValue != None:
                details.append(doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('TOWN')[0].firstChild.nodeValue)
        if doc.firstChild.getElementsByTagName('CONTRACTORS')[0].getElementsByTagName('POSTAL_CODE'):
            if doc.firstChild.getElementsByTagName('CONTRACTORS')[0].getElementsByTagName('POSTAL_CODE')[0].firstChild.nodeValue != None:
                details.append(doc.firstChild.getElementsByTagName('CONTRACTORS')[0].getElementsByTagName('POSTAL_CODE')[0].firstChild.nodeValue)
        if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('COUNTRY'):
            if doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('COUNTRY')[0].getAttribute('VALUE') != None:
                details.append(doc.firstChild.getElementsByTagName('CONTRACTORS')[EN_POS].getElementsByTagName('COUNTRY')[0].getAttribute('VALUE'))
        df.loc[0, 'CONTRACTOR ADDRESS DETAILS'] = ', '.join(details)

    # DATE OF PUBLICATION
    if doc.firstChild.getElementsByTagName('DATE_PUBLICATION_NOTICE'): 
        if doc.firstChild.getElementsByTagName('DATE_PUBLICATION_NOTICE')[0].firstChild != None:
            df.loc[0, 'DATE OF PUBLICATION'] = doc.firstChild.getElementsByTagName('DATE_PUBLICATION_NOTICE')[0].firstChild.nodeValue
    else:
        if doc.firstChild.getElementsByTagName('OBJECT_CONTRACT'):
            for obj in doc.firstChild.getElementsByTagName('OBJECT_CONTRACT')[EN_POS].childNodes:
                if obj.tagName == 'DATE_PUBLICATION_NOTICE':
                    df.loc[0, 'DATE OF PUBLICATION'] = obj.firstChild.nodeValue
        else:
            if doc.firstChild.getElementsByTagName('OTH_NOT'):
                for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                    if item.getAttribute('LG') == 'EN':
                        TI_MARK = item.getElementsByTagName('TI_MARK')
                        TXT_MARK = item.getElementsByTagName('TXT_MARK')
                        NO_MARK = item.getElementsByTagName('NO_MARK')
                        for i in range(len(item.getElementsByTagName('NO_MARK'))):
                            if TI_MARK[i].firstChild:
                                if ('publication' in TI_MARK[i].firstChild.nodeValue.lower()) & ('date' in TI_MARK[i].firstChild.nodeValue.lower()):
                                    childs_txt = []
                                    for child in TXT_MARK[i].childNodes:
                                        for kid in child.childNodes:
                                            if kid.nodeValue != None:
                                                childs_txt.append(kid.nodeValue)
                                    childs_txt = [str(n) if n is None else n for n in childs_txt]
                                    df.loc[0, 'DATE OF PUBLICATION'] = ', '.join(childs_txt)

    # TOTAL QUANTITY OR SCOPE
    if doc.firstChild.getElementsByTagName('TOTAL_QUANTITY_OR_SCOPE'):
        try:
            df.loc[0, 'TOTAL QUANTITY OR SCOPE'] = doc.firstChild.getElementsByTagName('TOTAL_QUANTITY_OR_SCOPE')[0].firstChild.firstChild.nodeValue
        except:
            try:
                df.loc[0, 'TOTAL QUANTITY OR SCOPE'] = doc.firstChild.getElementsByTagName('TOTAL_QUANTITY_OR_SCOPE')[0].firstChild.nodeValue
            except:
                df.loc[0, 'TOTAL QUANTITY OR SCOPE'] = ''

    # AWARD CRITERIA
    if doc.firstChild.getElementsByTagName('AC_AWARD_CRIT'): 
        if doc.firstChild.getElementsByTagName('AC_AWARD_CRIT')[0].firstChild != None:
            df.loc[0, 'AWARD CRITERIA'] = doc.firstChild.getElementsByTagName('AC_AWARD_CRIT')[0].firstChild.nodeValue

    # AWARD CRITERIA DETAILS
    if doc.firstChild.getElementsByTagName('AWARD_CRITERIA_DETAIL'):
        criterias, weights = [], []
        for element in doc.firstChild.getElementsByTagName('AWARD_CRITERIA_DETAIL'):
            for child in element.getElementsByTagName('CRITERIA'):
                criterias.append(child.firstChild.nodeValue)
            for child in element.getElementsByTagName('WEIGHTING'):
                weights.append(child.firstChild.nodeValue)
        for i in range(len(criterias)):
            df.loc[0, 'AWARD CRITERIA DETAILS'] = criterias[i] + ' : ' + weights[i]

    # CONTRACT DURATION
    if doc.firstChild.getElementsByTagName('DURATION'):
        df.loc[0, 'CONTRACT DURATION'] = doc.firstChild.getElementsByTagName('DURATION')[EN_POS].firstChild.nodeValue +\
        ' ' + doc.firstChild.getElementsByTagName('DURATION')[EN_POS].getAttribute('TYPE')
    else:
        if doc.firstChild.getElementsByTagName('OTH_NOT'):
            for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                if item.getAttribute('LG') == 'EN':
                    TI_MARK = item.getElementsByTagName('TI_MARK')
                    TXT_MARK = item.getElementsByTagName('TXT_MARK')
                    NO_MARK = item.getElementsByTagName('NO_MARK')
                    for i in range(len(item.getElementsByTagName('NO_MARK'))):
                        if TI_MARK[i].firstChild:
                            if TI_MARK[i].firstChild.nodeValue.lower() == 'duration of contract':
                                childs_txt = []
                                for child in TXT_MARK[i].childNodes: # txt
                                    for kid in child.childNodes:
                                        if kid.nodeValue != None:
                                            childs_txt.append(kid.nodeValue)
                                childs_txt = [str(n) if n is None else n for n in childs_txt]
                                df.loc[0, 'CONTRACT DURATION'] = ', '.join(childs_txt)

    # WORK DATE STARTING
    if doc.firstChild.getElementsByTagName('PERIOD_WORK_DATE_STARTING'):
        try:
            wrk_dt_st = []
            if doc.firstChild.getElementsByTagName('PERIOD_WORK_DATE_STARTING')[0].firstChild.firstChild.nodeValue is not None:
                wrk_dt_st.append(doc.firstChild.getElementsByTagName('PERIOD_WORK_DATE_STARTING')[0].firstChild.firstChild.nodeValue)
                wrk_dt_st.append(doc.firstChild.getElementsByTagName('PERIOD_WORK_DATE_STARTING')[0].firstChild.tagName)
            df.loc[0, 'WORK DATE STARTING'] = ' '.join(wrk_dt_st)
        except:
            df.loc[0, 'WORK DATE STARTING'] = ''
                       

    # START DATE
    if doc.firstChild.getElementsByTagName('DATE_START'): 
        if doc.firstChild.getElementsByTagName('DATE_START')[0].firstChild != None:
            df.loc[0, 'START DATE'] = doc.firstChild.getElementsByTagName('DATE_START')[0].firstChild.nodeValue
    elif doc.firstChild.getElementsByTagName('START_DATE'):
        try:
            st_dt = []
            st_dt.append(doc.firstChild.getElementsByTagName('START_DATE')[0].childNodes[0].firstChild.nodeValue)
            st_dt.append(doc.firstChild.getElementsByTagName('START_DATE')[0].childNodes[1].firstChild.nodeValue)
            st_dt.append(doc.firstChild.getElementsByTagName('START_DATE')[0].childNodes[2].firstChild.nodeValue)
            df.loc[0, 'START DATE'] = '-'.join(st_dt)
        except:
            df.loc[0, 'START DATE'] = ''

    # END DATE
    if doc.firstChild.getElementsByTagName('DATE_END'): 
        if doc.firstChild.getElementsByTagName('DATE_END')[0].firstChild != None:
            df.loc[0, 'END DATE'] = doc.firstChild.getElementsByTagName('DATE_END')[0].firstChild.nodeValue
    elif doc.firstChild.getElementsByTagName('END_DATE'):
        try:
            st_dt = []
            st_dt.append(doc.firstChild.getElementsByTagName('END_DATE')[0].childNodes[0].firstChild.nodeValue)
            st_dt.append(doc.firstChild.getElementsByTagName('END_DATE')[0].childNodes[1].firstChild.nodeValue)
            st_dt.append(doc.firstChild.getElementsByTagName('END_DATE')[0].childNodes[2].firstChild.nodeValue)
            df.loc[0, 'END DATE'] = '-'.join(st_dt)
        except:
            df.loc[0, 'END DATE'] = ''

    # ACCEPTED VARIANTS
    if doc.firstChild.getElementsByTagName('NO_ACCEPTED_VARIANTS'):
        df.loc[0, 'ACCEPTED VARIANTS'] = doc.firstChild.getElementsByTagName('NO_ACCEPTED_VARIANTS')[0].tagName
    else:
        if doc.firstChild.getElementsByTagName('ACCEPTED_VARIANTS'):
            df.loc[0, 'ACCEPTED VARIANTS'] = doc.firstChild.getElementsByTagName('ACCEPTED_VARIANTS')[0].getAttribute('VALUE')

    # OPTIONS
    if doc.firstChild.getElementsByTagName('NO_OPTIONS'):
        df.loc[0, 'OPTIONS'] = doc.firstChild.getElementsByTagName('NO_OPTIONS')[0].tagName
    else:
        if doc.firstChild.getElementsByTagName('OPTIONS'):
            for element in doc.firstChild.getElementsByTagName('OPTIONS'): 
                for child in element.childNodes:
                    if child.tagName == 'OPTION_DESCRIPTION': 
                        if child.firstChild.firstChild != None:
                            df.loc[0, 'OPTIONS'] = child.firstChild.firstChild.nodeValue

    # RENEWAL
    if doc.firstChild.getElementsByTagName('RECURRENT_CONTRACT'):
        df.loc[0, 'RENEWAL'] = 'Yes'

    # EU PROGR RELATED
    if doc.firstChild.getElementsByTagName('EU_PROGR_RELATED'):
        prog_names = []
        for element in doc.firstChild.getElementsByTagName('EU_PROGR_RELATED')[EN_POS].childNodes:
            for item in element.childNodes:
                if item.nodeValue != None:
                    prog_names.append(item.nodeValue)
        df.loc[0, 'EU PROGR RELATED'] = ','.join(prog_names)
    else:
        if doc.firstChild.getElementsByTagName('NO_EU_PROGR_RELATED'):
            df.loc[0, 'EU PROGR RELATED'] = doc.firstChild.getElementsByTagName('NO_EU_PROGR_RELATED')[0].tagName

    # SUITABILITY
    if doc.firstChild.getElementsByTagName('SUITABILITY'):
        suitability_list = []
        for element in doc.firstChild.getElementsByTagName('SUITABILITY')[EN_POS].childNodes:
            for item in element.childNodes:
                if item.nodeValue != None:
                    suitability_list.append(item.nodeValue)
        df.loc[0, 'SUITABILITY'] = ','.join(suitability_list)

    # ECONOMIC CRITERIA
    if doc.firstChild.getElementsByTagName('ECONOMIC_CRITERIA_DOC'):
        df.loc[0, 'ECONOMIC CRITERIA'] = doc.firstChild.getElementsByTagName('ECONOMIC_CRITERIA_DOC')[EN_POS].firstChild

    # TECHNICAL CRITERIA
    if doc.firstChild.getElementsByTagName('TECHNICAL_CRITERIA_DOC'):
        df.loc[0, 'TECHNICAL CRITERIA'] = doc.firstChild.getElementsByTagName('TECHNICAL_CRITERIA_DOC')[EN_POS].firstChild

    # PERFORMANCE CONDITIONS
    if doc.firstChild.getElementsByTagName('PERFORMANCE_CONDITIONS'):
        perf_cond = []
        for element in doc.firstChild.getElementsByTagName('PERFORMANCE_CONDITIONS')[EN_POS].childNodes:
            for item in element.childNodes:
                if item.nodeValue != None:
                    perf_cond.append(item.nodeValue)
        df.loc[0, 'PERFORMANCE CONDITIONS'] = ','.join(perf_cond)

    # PROCEDURE TYPE
    if doc.firstChild.getElementsByTagName('PT_OPEN'):
        df.loc[0, 'PROCEDURE TYPE'] = 'Open'
    elif doc.firstChild.getElementsByTagName('PT_RESTRICTED'):
        df.loc[0, 'PROCEDURE TYPE'] = 'Restricted'
    else:
        df.loc[0, 'PROCEDURE TYPE'] = ''

    # PROCEDURE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'procedure':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'PROCEDURE'] = ', '.join(childs_txt)
    
    # CONTRACT COVERED BY GPA
    if doc.firstChild.getElementsByTagName('NO_CONTRACT_COVERED_GPA'):
        df.loc[0, 'CONTRACT COVERED BY GPA'] = 'No'
    elif doc.firstChild.getElementsByTagName('CONTRACT_COVERED_GPA'):
        df.loc[0, 'CONTRACT COVERED BY GPA'] = 'Yes'
    else:
        df.loc[0, 'CONTRACT COVERED BY GPA'] = ''

    # FILE REF NUM
    if doc.firstChild.getElementsByTagName('FILE_REFERENCE_NUMBER'):
        try:
            df.loc[0, 'FILE REF NUM'] = doc.firstChild.getElementsByTagName('FILE_REFERENCE_NUMBER')[0].firstChild.firstChild.nodeValue
        except:
            df.loc[0, 'FILE REF NUM'] = ''

    # DATE OF RECEIPT OF TENDER
    if doc.firstChild.getElementsByTagName('DATE_RECEIPT_TENDERS'): 
        if doc.firstChild.getElementsByTagName('DATE_RECEIPT_TENDERS')[EN_POS].firstChild != None:
            df.loc[0, 'DATE OF RECEIPT OF TENDER'] = doc.firstChild.getElementsByTagName('DATE_RECEIPT_TENDERS')[EN_POS].firstChild.nodeValue

    # TIME OF RECEIPT OF TENDER
    if doc.firstChild.getElementsByTagName('TIME_RECEIPT_TENDERS'): 
        if doc.firstChild.getElementsByTagName('TIME_RECEIPT_TENDERS')[EN_POS].firstChild != None:
            df.loc[0, 'TIME OF RECEIPT OF TENDER'] = doc.firstChild.getElementsByTagName('TIME_RECEIPT_TENDERS')[EN_POS].firstChild.nodeValue

    # DURATION TENDER VALID
    if doc.firstChild.getElementsByTagName('DURATION_TENDER_VALID'): 
        if doc.firstChild.getElementsByTagName('DURATION_TENDER_VALID')[EN_POS].firstChild != None:
            df.loc[0, 'DURATION TENDER VALID'] = doc.firstChild.getElementsByTagName('DURATION_TENDER_VALID')[EN_POS].firstChild.nodeValue \
            + ' ' + doc.firstChild.getElementsByTagName('DURATION_TENDER_VALID')[EN_POS].getAttribute('TYPE')

    # MINIMUM TIME MAINTAINING TENDER
    if doc.firstChild.getElementsByTagName('MINIMUM_TIME_MAINTAINING_TENDER'): 
        if doc.firstChild.getElementsByTagName('MINIMUM_TIME_MAINTAINING_TENDER')[0].firstChild.firstChild != None:
            df.loc[0, 'MINIMUM TIME MAINTAINING TENDER'] = doc.firstChild.getElementsByTagName('MINIMUM_TIME_MAINTAINING_TENDER')[0].firstChild.firstChild.nodeValue \
            + ' ' + doc.firstChild.getElementsByTagName('MINIMUM_TIME_MAINTAINING_TENDER')[0].firstChild.tagName

    # DATE OF OPENING OF TENDER
    if doc.firstChild.getElementsByTagName('DATE_OPENING_TENDERS'): 
        if doc.firstChild.getElementsByTagName('DATE_OPENING_TENDERS')[EN_POS].firstChild != None:
            df.loc[0, 'DATE OF OPENING OF TENDER'] = doc.firstChild.getElementsByTagName('DATE_OPENING_TENDERS')[EN_POS].firstChild.nodeValue

    # TIME OF OPENING OF TENDER
    if doc.firstChild.getElementsByTagName('TIME_OPENING_TENDERS'): 
        if doc.firstChild.getElementsByTagName('TIME_OPENING_TENDERS')[EN_POS].firstChild != None:
            df.loc[0, 'TIME OF OPENING OF TENDER'] = doc.firstChild.getElementsByTagName('TIME_OPENING_TENDERS')[EN_POS].firstChild.nodeValue

    # PLACE OF OPENING TENDER
    if doc.firstChild.getElementsByTagName('PLACE'):
        if doc.firstChild.getElementsByTagName('PLACE')[EN_POS].firstChild.firstChild != None:
            df.loc[0, 'PLACE OF OPENING TENDER'] = doc.firstChild.getElementsByTagName('PLACE')[EN_POS].firstChild.firstChild.nodeValue

    # NOTICE DISPATCH DATE
    if doc.firstChild.getElementsByTagName('DATE_DISPATCH_NOTICE'): 
        if doc.firstChild.getElementsByTagName('DATE_DISPATCH_NOTICE')[0].firstChild != None:
            df.loc[0, 'NOTICE DISPATCH DATE'] = doc.firstChild.getElementsByTagName('DATE_DISPATCH_NOTICE')[0].firstChild.nodeValue
    else: 
        if doc.firstChild.getElementsByTagName('NOTICE_DISPATCH_DATE'):
            df.loc[0, 'NOTICE DISPATCH DATE'] = '{}-{}-{}'.format(
                doc.firstChild.getElementsByTagName('NOTICE_DISPATCH_DATE')[0].childNodes[0].firstChild.nodeValue, 
                doc.firstChild.getElementsByTagName('NOTICE_DISPATCH_DATE')[0].childNodes[1].firstChild.nodeValue, 
                doc.firstChild.getElementsByTagName('NOTICE_DISPATCH_DATE')[0].childNodes[2].firstChild.nodeValue)

    # INVITATION DISPATCH DATE
    if doc.firstChild.getElementsByTagName('DISPATCH_INVITATIONS_DATE'):
        df.loc[0, 'INVITATION DISPATCH DATE'] = '{}-{}-{}'.format(
            doc.firstChild.getElementsByTagName('DISPATCH_INVITATIONS_DATE')[0].getElementsByTagName('DAY')[0].firstChild.nodeValue,
            doc.firstChild.getElementsByTagName('DISPATCH_INVITATIONS_DATE')[0].getElementsByTagName('MONTH')[0].firstChild.nodeValue,
            doc.firstChild.getElementsByTagName('DISPATCH_INVITATIONS_DATE')[0].getElementsByTagName('YEAR')[0].firstChild.nodeValue
        )

    # ADDITIONAL INFO
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'additional information':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'ADDITIONAL INFO'] = ', '.join(childs_txt)
    

    # ADD COMPLEMENTARY INFO
    if doc.firstChild.getElementsByTagName('COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE'):
        if doc.firstChild.getElementsByTagName('COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE')[0]\
        .getElementsByTagName('ADDITIONAL_INFORMATION'):
            info = []
            for child in doc.firstChild.getElementsByTagName('COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE')[0]\
            .getElementsByTagName('ADDITIONAL_INFORMATION')[0].childNodes:
                if child.firstChild != None:
                    info.append(child.firstChild.nodeValue)
            if len(info) > 0:
                df.loc[0, 'ADD COMPLEMENTARY INFO'] = ' '.join(info)

    # LEGAL BASIS
    if doc.firstChild.getElementsByTagName('LEGAL_BASIS'): 
        if doc.firstChild.getElementsByTagName('LEGAL_BASIS')[0] != None:
            df.loc[0, 'LEGAL BASIS'] = doc.firstChild.getElementsByTagName('LEGAL_BASIS')[0].nodeValue
    else:
        if doc.firstChild.getElementsByTagName('OTH_NOT'):
            for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
                if item.getAttribute('LG') == 'EN':
                    TI_MARK = item.getElementsByTagName('TI_MARK')
                    TXT_MARK = item.getElementsByTagName('TXT_MARK')
                    NO_MARK = item.getElementsByTagName('NO_MARK')
                    for i in range(len(item.getElementsByTagName('NO_MARK'))):
                        if TI_MARK[i].firstChild:
                            if 'legal basis' in TI_MARK[i].firstChild.nodeValue.lower():
                                childs_txt = []
                                for child in TXT_MARK[i].childNodes: # txt
                                    for kid in child.childNodes:
                                        if kid.nodeValue != None:
                                            childs_txt.append(kid.nodeValue)
                                childs_txt = [str(n) if n is None else n for n in childs_txt]
                                df.loc[0, 'LEGAL BASIS'] = ', '.join(childs_txt)

    # PUBLICATION REF
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'publication reference':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'PUBLICATION REF'] = ', '.join(childs_txt)
                        
    # FINANCING
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'financing':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'FINANCING'] = ', '.join(childs_txt)

    # INDICATIVE BUDGET
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'indicative budget':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'INDICATIVE BUDGET'] = ', '.join(childs_txt)
    
    # INTENDED PUBLICATION DATE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'intended timing of publication of the contract notice':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'INTENDED PUBLICATION DATE'] = ', '.join(childs_txt)

    # NUMBER AND TITLES OF LOTS
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if (TI_MARK[i].firstChild.nodeValue.lower() == 'number and titles of lots') | (TI_MARK[i].firstChild.nodeValue.lower() == 'lot number and lot title'):
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'NUMBER AND TITLES OF LOTS'] = ', '.join(childs_txt)

    # MAXIMUM BUDGET
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'maximum budget':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'MAXIMUM BUDGET'] = ', '.join(childs_txt)

    # SCOPE FOR ADDITIONAL SERVICE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'scope for additional services':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'SCOPE FOR ADDITIONAL SERVICE'] = ', '.join(childs_txt)

    # ELIGIBILITY
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'eligibility':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'ELIGIBILITY'] = ', '.join(childs_txt)

    # CANDIDATURE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'candidature':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'CANDIDATURE'] = ', '.join(childs_txt)

    # NUM OF APPLICATIONS
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'number of applications':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'NUM OF APPLICATIONS'] = ', '.join(childs_txt)

    # SHORTLIST ALLIANCES PROHIBITED
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'shortlist alliances prohibited':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'SHORTLIST ALLIANCES PROHIBITED'] = ', '.join(childs_txt)

    # GROUNDS FOR EXCLUSION
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'grounds for exclusion':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'GROUNDS FOR EXCLUSION'] = ', '.join(childs_txt)

    # SUB CONTRACTING
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'sub-contracting':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'SUB CONTRACTING'] = ', '.join(childs_txt)

    # NUM OF CANDIDATES TO BE SHORTLISTED
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'number of candidates to be short-listed':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'NUM OF CANDIDATES TO BE SHORTLISTED'] = ', '.join(childs_txt)

    # PROVISIONAL DATE OF INVITATION OF TENDER
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'provisional date of invitation to tender':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'PROVISIONAL DATE OF INVITATION OF TENDER'] = ', '.join(childs_txt)

    # PROVISIONAL DATE OF COMMENCEMENT OF TENDER
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'provisional commencement date of the contract':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'PROVISIONAL DATE OF COMMENCEMENT OF TENDER'] = ', '.join(childs_txt)

    # INITIAL PERIOD OF IMPLEMENTATION OF TASK
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'initial period of implementation of tasks':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'INITIAL PERIOD OF IMPLEMENTATION OF TASK'] = ', '.join(childs_txt)

    # SELECTION CRITERIA
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'selection criteria':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'SELECTION CRITERIA'] = ', '.join(childs_txt)

    # AWARD CRITERIA
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'award criteria':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'AWARD CRITERIA'] = ', '.join(childs_txt)

    # APPLICATION SUBMISSION DEADLINE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'deadline for submission of applications':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'APPLICATION SUBMISSION DEADLINE'] = ', '.join(childs_txt)

    # ALTERATION OR WITHDRAWAL
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'alteration or withdrawal of applications':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'ALTERATION OR WITHDRAWAL'] = ','.join(childs_txt)

    # OPERATIONAL LANG
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'operational language':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'OPERATIONAL LANG'] = ', '.join(childs_txt)

    # CONTRACT NUM AND VALUE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'contract number and value':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'CONTRACT NUM AND VALUE'] = ', '.join(childs_txt)

    # CONTRACT AWARD DATE
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'date of award of the contract':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'CONTRACT AWARD DATE'] = ', '.join(childs_txt)

    # NUM OF TENDERS RECEIVED
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'number of tenders received':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'NUM OF TENDERS RECEIVED'] = ', '.join(childs_txt)

    # OVERALL SCORE OF CHOSEN TENDER
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if TI_MARK[i].firstChild.nodeValue.lower() == 'overall score of chosen tender':
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'OVERALL SCORE OF CHOSEN TENDER'] = ', '.join(childs_txt)

    # SUCCESSFUL TENDERER DETAILS
    if doc.firstChild.getElementsByTagName('OTH_NOT'):
        for item in doc.firstChild.getElementsByTagName('OTH_NOT'):
            if item.getAttribute('LG') == 'EN':
                TI_MARK = item.getElementsByTagName('TI_MARK')
                TXT_MARK = item.getElementsByTagName('TXT_MARK')
                NO_MARK = item.getElementsByTagName('NO_MARK')
                for i in range(len(item.getElementsByTagName('NO_MARK'))):
                    if TI_MARK[i].firstChild:
                        if 'successful tenderer' in TI_MARK[i].firstChild.nodeValue.lower():
                            childs_txt = []
                            for child in TXT_MARK[i].childNodes: # txt
                                for kid in child.childNodes:
                                    if kid.nodeValue != None:
                                        childs_txt.append(kid.nodeValue)
                            childs_txt = [str(n) if n is None else n for n in childs_txt]
                            df.loc[0, 'SUCCESSFUL TENDERER DETAILS'] = ', '.join(childs_txt)
                        
    return df

def process(dirName):
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(dirName):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
    
    # extract tarfiles
    for file in listOfFiles:
        if file.endswith(".tar.gz"):
            
            # output directory to store extracted directories
            if not os.path.exists('extractedFiles'):
                os.mkdir('extractedFiles')
            else:
                # remove all files in 'extractedFiles' dir from last run
                dirs = list()
                for (dirpath, dirnames, filenames) in os.walk('extractedFiles'):
                    dirs += [os.path.join(dirpath) for f in filenames]
                print('[info] removing files from past run..', end=" ")
                for _dir in set(dirs):
                    shutil.rmtree(_dir)
                print('Done!')
            
            df = pd.DataFrame()
            
            tar = tarfile.open(file, "r:gz")
            print('[info] extracting all xml files in {}..'.format(file), end=" ")
            tar.extractall('extractedFiles/')
            print('Done!')
            
            # iterate through each directory
            xmlFilesList = list()
            for (dirpath, dirnames, filenames) in os.walk('extractedFiles'):
                xmlFilesList += [os.path.join(dirpath, file) for file in filenames]
        
            # iterate through each xml file in each directory & write to 1 csv file per directory
            for i, each in enumerate(xmlFilesList):
                print('[info] parsing {} file...'.format(each), end=" ")
                row = parseXML(each)
                if i==0:
                    df=row
                else:
                    df = pd.concat([df, row], axis=0, ignore_index=True, sort=True)
                print('[Done]')
                # os.system('cls') # flush stdout
                
                if i%100==0:
                    file_name = file.split('.tar.gz')[0] + '.csv'
                    df.to_csv(file_name, index=False)

if __name__ == "__main__":

    dirname = "FTP_Data/2019"
    process(dirname)
    print("Parsing successfully completed!")
