import xmltodict
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime as dt

# the CODED_DATA & TRANSLATION_SECTION
def coded_and_translation_data(doc, EN_POS):
    d = dict() 
    # DOC_ID
    d['DOC_ID'] = doc['@DOC_ID']
    
    # EDITION
    d['EDITION'] = doc['@EDITION']
        
    # CODED_DATA_SECTION - REF_OJS
    d['COLL_OJ'] = doc['CODED_DATA_SECTION']['REF_OJS']['COLL_OJ']
    d['No_OJ'] = doc['CODED_DATA_SECTION']['REF_OJS']['NO_OJ']
    d['DATE_PUB'] = doc['CODED_DATA_SECTION']['REF_OJS']['DATE_PUB']
    
    # CODED_DATA_SECTION - NOTICE_DATA
    d['NO_DOC_OJS'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['NO_DOC_OJS']
    try:
        d['URL'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['URI_LIST']['URI_DOC'][EN_POS]['#text']
    except:
        d['URL'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['URI_LIST']['URI_DOC']['#text']
        
    d['LG_ORIG'] = ','.join(doc['CODED_DATA_SECTION']['NOTICE_DATA']['LG_ORIG']) if isinstance(doc['CODED_DATA_SECTION']['NOTICE_DATA']['LG_ORIG'], list) else doc['CODED_DATA_SECTION']['NOTICE_DATA']['LG_ORIG']
    d['ISO_COUNTRY'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['ISO_COUNTRY']['@VALUE']
    d['ORIGINAL_CPV_CODE'] = ';'.join(cpv['@CODE'] for cpv in doc['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']) if isinstance(doc['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV'], list) else doc['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']['@CODE']
    d['ORIGINAL_CPV'] = ';'.join(cpv['#text'] for cpv in doc['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']) if isinstance(doc['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV'], list) else doc['CODED_DATA_SECTION']['NOTICE_DATA']['ORIGINAL_CPV']['#text']
    if 'n2016:PERFORMANCE_NUTS' in doc['CODED_DATA_SECTION']['NOTICE_DATA'].keys():
        if isinstance(doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:PERFORMANCE_NUTS'], dict):
            d['n2016-PERFORMANCE_NUTS_CODE'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:PERFORMANCE_NUTS']['@CODE']
            d['n2016-PERFORMANCE_NUTS'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:PERFORMANCE_NUTS']['#text']
        else:
            d['n2016-PERFORMANCE_NUTS_CODE'] = ','.join([item['@CODE'] for item in doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:PERFORMANCE_NUTS']])
            d['n2016-PERFORMANCE_NUTS'] = ','.join([item['#text'] for item in doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:PERFORMANCE_NUTS']])
    if 'n2016:CA_CE_NUTS' in doc['CODED_DATA_SECTION']['NOTICE_DATA'].keys():
        if isinstance(doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:CA_CE_NUTS'], dict):
            d['n2016-CA_CE_NUTS_CODE'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:CA_CE_NUTS']['@CODE']
            d['n2016-CA_CE_NUTS'] = doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:CA_CE_NUTS']['#text']
        elif isinstance(doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:CA_CE_NUTS'], list):
            d['n2016-CA_CE_NUTS_CODE'] = ','.join(item['@CODE'] for item in doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:CA_CE_NUTS'])
            d['n2016-CA_CE_NUTS'] = ','.join(item['#text'] for item in doc['CODED_DATA_SECTION']['NOTICE_DATA']['n2016:CA_CE_NUTS'])
    
    # CODED_DATA_SECTION - CODIF_DATA
    d['DS_DATE_DISPATCH'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['DS_DATE_DISPATCH'] if 'DS_DATE_DISPATCH' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    d['DT_DATE_FOR_SUBMISSION'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['DT_DATE_FOR_SUBMISSION'] if 'DT_DATE_FOR_SUBMISSION' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    d['AA_AUTHORITY_TYPE'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['AA_AUTHORITY_TYPE']['#text']
    d['AA_AUTHORITY_TYPE_CODE'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['AA_AUTHORITY_TYPE']['@CODE']
    d['TD_DOCUMENT_TYPE'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['TD_DOCUMENT_TYPE']['#text']
    d['NC_CONTRACT_NATURE'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['NC_CONTRACT_NATURE']['#text']
    d['PR_PROC'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['PR_PROC']['#text'] if 'PR_PROC' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    d['RP_REGULATION'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['RP_REGULATION']['#text'] if 'RP_REGULATION' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    d['RP_REGULATION_CODE'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['RP_REGULATION']['@CODE'] if 'RP_REGULATION' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    d['TY_TYPE_BID'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['TY_TYPE_BID']['#text'] if 'TY_TYPE_BID' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    d['AC_AWARD_CRIT'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['AC_AWARD_CRIT']['#text'] if 'AC_AWARD_CRIT' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    if 'MA_MAIN_ACTIVITIES' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys():
        if isinstance(doc['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES'], dict):
            d['MA_MAIN_ACTIVITIES'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES']['#text']
        else:
            d['MA_MAIN_ACTIVITIES'] = ','.join(item['#text'] for item in doc['CODED_DATA_SECTION']['CODIF_DATA']['MA_MAIN_ACTIVITIES'])
    d['HEADING'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['HEADING']
    d['INITIATOR'] = doc['CODED_DATA_SECTION']['CODIF_DATA']['INITIATOR'] if 'INITIATOR' in doc['CODED_DATA_SECTION']['CODIF_DATA'].keys() else ''
    
    # TRANSLATION_SECTION - ML_TITLES
    d['TITLE'] = doc['TRANSLATION_SECTION']['ML_TITLES']['ML_TI_DOC'][EN_POS]['TI_TEXT']['P']
    d['CITY'] = doc['TRANSLATION_SECTION']['ML_TITLES']['ML_TI_DOC'][EN_POS]['TI_CY']
    d['TOWN'] = doc['TRANSLATION_SECTION']['ML_TITLES']['ML_TI_DOC'][EN_POS]['TI_TOWN']
    
    # TRANSLATION_SECTION - ML_AA_NAMES
    try:
        d['CONTRACTING AUTHORITY'] = doc['TRANSLATION_SECTION']['ML_AA_NAMES']['AA_NAME'][EN_POS]['#text']
    except:
        d['CONTRACTING AUTHORITY'] = doc['TRANSLATION_SECTION']['ML_AA_NAMES']['AA_NAME']['#text']
        
    return d

def R209_CAN(doc, EN_POS):
    d = dict()
    if '@VERSION' in doc.keys():
        d['VERSION'] = doc['@VERSION']
    else:
        if isinstance(doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]], dict):
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]]['@VERSION']
        else:
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]][0]['@VERSION']
    if isinstance(doc['FORM_SECTION']['F03_2014'], list):
        d['FORM'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['@FORM']
        d['LEGAL_BASIS'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['LEGAL_BASIS']['@VALUE']
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['n2016-NUTS'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['n2016:NUTS']['@CODE']
        d['CA_TYPE'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['CA_TYPE']['@VALUE'] if 'CA_TYPE' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['CA_TYPE_OTHER'] if 'CA_TYPE_OTHER' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY']['CA_ACTIVITY_OTHER'] if 'CA_ACTIVITY_OTHER' in doc['FORM_SECTION']['F03_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CONTRACT_TITLE'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['TITLE']['P']
        d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['REFERENCE_NUMBER'] if 'REFERENCE_NUMBER' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
        d['CPV_MAIN'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['CPV_MAIN']['CPV_CODE']['@CODE']
        d['TYPE_CONTRACT'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['TYPE_CONTRACT']['@CTYPE']
        d['TOTAL_VAL_CURR'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['VAL_TOTAL']['@CURRENCY'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
        d['TOTAL_VAL'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['VAL_TOTAL']['#text'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
        shrt_dsc = []
        if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'], list):
            for each in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P']:
                if isinstance(each, dict):
                    shrt_dsc.append(each['#text'])
                else:
                    shrt_dsc.append(each)
        elif isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'], dict):
            shrt_dsc.append(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P']['#text'])
        else:
            shrt_dsc.append(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'])
        d['SHORT_DESCR'] = ','.join([str(x) for x in shrt_dsc])
        if 'NO_LOT_DIVISION' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT'].keys():
            d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['NO_LOT_DIVISION']
        if 'LOT_DIVISION' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT'].keys():
            if doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'] is not None:
                d['MAX_LOT_DIVISION'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_NUMBER'] if 'LOT_MAX_NUMBER' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                d['MAX_LOT_PER_TENDERER'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'] if 'LOT_MAX_ONE_TENDERER' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                d['LOT_ALL'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_ALL'] if 'LOT_ALL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''

        if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], dict):
            if 'MAIN_SITE' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                    main_site = []
                    for item in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']:
                        if isinstance(item, dict):
                            main_site.append(item['#text'])
                        elif isinstance(item, str):
                            main_site.append(item)
                        else:
                            main_site.append(str(item))
                    d['MAIN_SITE'] = ', '.join(main_site)
                elif isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']['#text']
                else:
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']
            if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys() else ''
                else:
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']]) 
            if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
            else:
                d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
                d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([s['#text'] if isinstance(s, dict) else s for s in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']])
                if 'DURATION' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                    d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']

                elif isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
                    if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                        if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                            d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS]['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS].keys() else ''
                        else:
                            d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS]['CPV_ADDITIONAL']])
                    if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                        d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
                    else:
                        d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
                    d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([s['#text'] if isinstance(s, dict) else s for s in doc['FORM_SECTION'][EN_POS]['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS]['SHORT_DESCR']['P']])
                    if 'DURATION' in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS].keys():
                        d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS]['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'][EN_POS]['DURATION']['@TYPE']

        elif isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
            lot_title, cpv_additional, lot_no, n2016_nuts, short_descr, main_site = [],[],[],[],[],[]
            for item in doc['FORM_SECTION']['F03_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']:
                if 'MAIN_SITE' in item.keys():
                    if isinstance(item['MAIN_SITE']['P'], list):
                        for ii in item['MAIN_SITE']['P']:
                            if isinstance(ii, dict):
                                main_site.append(ii['#text'])
                            elif isinstance(ii, str):
                                main_site.append(ii)
                            else:
                                main_site.append(str(ii))
                    elif isinstance(item['MAIN_SITE']['P'], dict):
                        main_site.append(item['MAIN_SITE']['P']['#text'])
                    else:
                        main_site.append(item['MAIN_SITE']['P'])
                if 'LOT_NO' in item.keys():
                    if isinstance(item['LOT_NO'], dict):
                        lot_no.append(item['LOT_NO'])
                    else:
                        lot_no.append(item['LOT_NO'][0])
                if 'TITLE' in item.keys():
                    if isinstance(item['TITLE'], dict):
                        if isinstance(item['TITLE']['P'], dict):
                            lot_title.append(item['TITLE']['P']['#text'])
                        else:
                            lot_title.append(item['TITLE']['P'])
                    else:
                        lot_title.append(', '.join([i['P'] for i in item['TITLE']]))
                if 'CPV_ADDITIONAL' in item.keys():
                    if isinstance(item['CPV_ADDITIONAL'], dict):
                        cpv_additional.append(item['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                    else:
                        cpv_additional.append(', '.join([i['CPV_CODE']['@CODE'] for i in item['CPV_ADDITIONAL']]))
                if 'n2016:NUTS' in item.keys():
                    if isinstance(item['n2016:NUTS'], dict):
                        n2016_nuts.append(item['n2016:NUTS']['@CODE'])
                    else:
                        n2016_nuts.append(', '.join([i['@CODE'] for i in item['n2016:NUTS']]))
                if 'SHORT_DESCR' in item.keys():
                    if isinstance(item['SHORT_DESCR'], dict):
                        if isinstance(item['SHORT_DESCR']['P'], str):
                            short_descr.append(item['SHORT_DESCR']['P'])
                        elif isinstance(item['SHORT_DESCR']['P'], list):
                            for each in item['SHORT_DESCR']['P']:
                                short_descr.append(each)
                        else:
                            short_descr.append(item['SHORT_DESCR']['P']['#text'])
                else:
                    short_descr.append(', '.join([i['P'] for i in item['SHORT_DESCR']]))
            d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site])
            d['LOT_NO'] = '; '.join(lot_no)
            d['LOT_TITLES'] = '; '.join([str(x) if x is None else x for x in lot_title])
            d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_additional)
            d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(n2016_nuts)
            d['OBJECT_DESCR_SHORT_DESCR'] = ','.join([','.join(i) for i in short_descr if isinstance(i, list)])
            
        if isinstance(doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT'], list):
            awarded_contract_lots, awarded_contract_titles, num_of_awarded_contract = [], [], []
            for each in doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT']:
                awarded_contract_lots.append(each['LOT_NO']) if 'LOT_NO' in each.keys() else ''
                if 'TITLE' in each.keys():
                    if isinstance(each['TITLE']['P'], str):
                        awarded_contract_titles.append(each['TITLE']['P'])
                    else:
                        awarded_contract_titles.append(each['TITLE']['P']['#text'])
                if 'NO_AWARDED_CONTRACT' in each.keys():
                    if 'PROCUREMENT_UNSUCCESSFUL' in each['NO_AWARDED_CONTRACT'].keys():
                        num_of_awarded_contract.append(each['NO_AWARDED_CONTRACT']['PROCUREMENT_UNSUCCESSFUL'])
            d['AWARDED_CONTRACT_LOTS'] = '; '.join(awarded_contract_lots)
            d['AWARDED_CONTRACT_TITLES'] = '; '.join(awarded_contract_titles)
            d['AWARDED_CONTRACT_PROCUREMENT_UNSUCCESSFUL'] = '; '.join([str(num) for num in num_of_awarded_contract])
        else:
            d['AWARDED_CONTRACT_LOTS'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT']['LOT_NO'] if 'LOT_NO' in doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT'].keys() else ''
            d['AWARDED_CONTRACT_TITLES'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT']['TITLE']['P'] if 'TITLE' in doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT'].keys() else ''
            if 'NO_AWARDED_CONTRACT' in doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT'].keys():
                if 'PROCUREMENT_UNSUCCESSFUL' in doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT']['NO_AWARDED_CONTRACT'].keys():
                    d['AWARDED_CONTRACT_PROCUREMENT_UNSUCCESSFUL'] = doc['FORM_SECTION']['F03_2014'][EN_POS]['AWARD_CONTRACT']['NO_AWARDED_CONTRACT']['PROCUREMENT_UNSUCCESSFUL']

    elif isinstance(doc['FORM_SECTION']['F03_2014'], dict):
        d['FORM'] = doc['FORM_SECTION']['F03_2014']['@FORM']
        d['LEGAL_BASIS'] = doc['FORM_SECTION']['F03_2014']['LEGAL_BASIS']['@VALUE']
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['n2016-NUTS'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['n2016:NUTS']['@CODE']
        d['CA_TYPE'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['CA_TYPE']['@VALUE'] if 'CA_TYPE' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['CA_TYPE_OTHER'] if 'CA_TYPE_OTHER' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY']['CA_ACTIVITY_OTHER'] if 'CA_ACTIVITY_OTHER' in doc['FORM_SECTION']['F03_2014']['CONTRACTING_BODY'].keys() else ''
        d['CONTRACT_TITLE'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['TITLE']['P']
        d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['REFERENCE_NUMBER'] if 'REFERENCE_NUMBER' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT'].keys() else ''
        d['CPV_MAIN'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['CPV_MAIN']['CPV_CODE']['@CODE']
        d['TYPE_CONTRACT'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['TYPE_CONTRACT']['@CTYPE']
        d['TOTAL_VAL_CURR'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['VAL_TOTAL']['@CURRENCY'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT'].keys() else ''
        d['TOTAL_VAL'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['VAL_TOTAL']['#text'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT'].keys() else ''
        shrt_dsc = []
        if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'], list):
            for each in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P']:
                if isinstance(each, dict):
                    shrt_dsc.append(each['#text'])
                else:
                    shrt_dsc.append(each)
        elif isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'], dict):
            shrt_dsc.append(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P']['#text'])
        else:
            shrt_dsc.append(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'])
        d['SHORT_DESCR'] = ','.join([str(x) for x in shrt_dsc])
        if 'NO_LOT_DIVISION' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT'].keys():
                d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['NO_LOT_DIVISION']
        if 'LOT_DIVISION' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT'].keys():
            if doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION'] is not None:
                d['MAX_LOT_DIVISION'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_NUMBER'] if 'LOT_MAX_NUMBER' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                d['MAX_LOT_PER_TENDERER'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'] if 'LOT_MAX_ONE_TENDERER' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                d['LOT_ALL'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_ALL'] if 'LOT_ALL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
        if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], dict):
            if 'MAIN_SITE' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                    main_site = []
                    for item in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']:
                        if isinstance(item, dict):
                            main_site.append(item['#text'])
                        elif isinstance(item, str):
                            main_site.append(item)
                        else:
                            main_site.append(str(item))
                    d['MAIN_SITE'] = ','.join(main_site)
                elif isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']['#text']
                else:
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']
            if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys() else ''
                else:
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']])
            if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
            else:
                d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
                d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([s['#text'] if isinstance(s, dict) else str(s) for s in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']])
                if 'DURATION' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                    d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']

                elif isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
                    if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                        if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                            d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys() else ''
                        else:
                            d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']])
                    if isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                        d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
                    else:
                        d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
                    d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([s['#text'] if isinstance(s, dict) else s for s in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']])
                    if 'DURATION' in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                        d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']
        elif isinstance(doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
            lot_title, cpv_additional, lot_no, n2016_nuts, short_descr, main_site = [],[],[],[],[],[]
            for item in doc['FORM_SECTION']['F03_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']:
                if 'MAIN_SITE' in item.keys():
                    if isinstance(item['MAIN_SITE']['P'], list):
                        for ii in item['MAIN_SITE']['P']:
                            if isinstance(ii, dict):
                                main_site.append(ii['#text'])
                            elif isinstance(ii, str):
                                main_site.append(ii)
                            else:
                                main_site.append(str(ii))
                        main_site.append(','.join(main_site))
                    elif isinstance(item['MAIN_SITE']['P'], dict):
                        main_site.append(item['MAIN_SITE']['P']['#text'])
                    else:
                        main_site.append(item['MAIN_SITE']['P'])
                if 'LOT_NO' in item.keys():
                    if isinstance(item['LOT_NO'], dict):
                        lot_no.append(item['LOT_NO'])
                    else:
                        lot_no.append(item['LOT_NO'][0])
                if 'TITLE' in item.keys():
                    if isinstance(item['TITLE'], dict):
                        if isinstance(item['TITLE']['P'], dict):
                            lot_title.append(item['TITLE']['P']['#text'])
                        else:
                            lot_title.append(item['TITLE']['P'])
                    else:
                        lot_title.append(', '.join([i['P'] for i in item['TITLE']]))
                if 'CPV_ADDITIONAL' in item.keys():
                    if isinstance(item['CPV_ADDITIONAL'], dict):
                        cpv_additional.append(item['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                    else:
                        cpv_additional.append(', '.join([i['CPV_CODE']['@CODE'] for i in item['CPV_ADDITIONAL']]))
                if 'n2016:NUTS' in item.keys():
                    if isinstance(item['n2016:NUTS'], dict):
                        n2016_nuts.append(item['n2016:NUTS']['@CODE'])
                    else:
                        n2016_nuts.append(', '.join([i['@CODE'] for i in item['n2016:NUTS']]))
                if 'SHORT_DESCR' in item.keys():
                    if isinstance(item['SHORT_DESCR'], dict):
                        if isinstance(item['SHORT_DESCR']['P'], str):
                            short_descr.append(item['SHORT_DESCR']['P'])
                        elif isinstance(item['SHORT_DESCR']['P'], list):
                            for each in item['SHORT_DESCR']['P']:
                                short_descr.append(each)
                        else:
                            if item['SHORT_DESCR']['P'] is not None:
                                short_descr.append(item['SHORT_DESCR']['P']['#text'])
                            else:
                                short_descr.append('None')
                else:
                    short_descr.append(', '.join([i['P'] for i in item['SHORT_DESCR']]))
            d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site])
            d['LOT_NO'] = '; '.join(lot_no)
            d['LOT_TITLES'] = '; '.join([str(x) if x is None else x for x in lot_title])
            d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_additional)
            d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(n2016_nuts)
            d['OBJECT_DESCR_SHORT_DESCR'] = ','.join([','.join(i) for i in short_descr if isinstance(i, list)])
            
        if isinstance(doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT'], list):
            awarded_contract_lots, awarded_contract_titles, num_of_awarded_contract = [], [], []
            for each in doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT']:
                awarded_contract_lots.append(each['LOT_NO']) if 'LOT_NO' in each.keys() else ''
                if 'TITLE' in each.keys():
                    if isinstance(each['TITLE']['P'], str):
                        awarded_contract_titles.append(each['TITLE']['P'])
                    else:
                        awarded_contract_titles.append(each['TITLE']['P']['#text'])
                if 'NO_AWARDED_CONTRACT' in each.keys():
                    if 'PROCUREMENT_UNSUCCESSFUL' in each['NO_AWARDED_CONTRACT'].keys():
                        num_of_awarded_contract.append(each['NO_AWARDED_CONTRACT']['PROCUREMENT_UNSUCCESSFUL'])
            d['AWARDED_CONTRACT_LOTS'] = '; '.join(awarded_contract_lots)
            d['AWARDED_CONTRACT_TITLES'] = '; '.join(awarded_contract_titles)
            d['AWARDED_CONTRACT_PROCUREMENT_UNSUCCESSFUL'] = '; '.join([str(num) for num in num_of_awarded_contract])
        else:
            d['AWARDED_CONTRACT_LOTS'] = doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT']['LOT_NO'] if 'LOT_NO' in doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT'].keys() else ''
            d['AWARDED_CONTRACT_TITLES'] = doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT']['TITLE']['P'] if 'TITLE' in doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT'].keys() else ''
            if 'NO_AWARDED_CONTRACT' in doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT'].keys():
                if 'PROCUREMENT_UNSUCCESSFUL' in doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT']['NO_AWARDED_CONTRACT'].keys():
                    d['AWARDED_CONTRACT_PROCUREMENT_UNSUCCESSFUL'] = doc['FORM_SECTION']['F03_2014']['AWARD_CONTRACT']['NO_AWARDED_CONTRACT']['PROCUREMENT_UNSUCCESSFUL']
            
    return d

def R209_CN(doc, EN_POS):
    d = dict()
    if '@VERSION' in doc.keys():
        d['VERSION'] = doc['@VERSION']
    else:
        if isinstance(doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]], dict):
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]]['@VERSION']
        else:
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]][0]['@VERSION']
    if isinstance(doc['FORM_SECTION']['F02_2014'], list):
        d['FORM'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['@FORM']
        d['LEGAL_BASIS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['LEGAL_BASIS']['@VALUE'] if 'LEGAL_BASIS' in doc['FORM_SECTION']['F02_2014'][EN_POS].keys() else ''
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['n2016-NUTS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['n2016:NUTS']['@CODE']
        d['CA_TYPE'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['CA_TYPE']['@VALUE'] if 'CA_TYPE' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['CA_TYPE_OTHER'] if 'CA_TYPE_OTHER' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY']['CA_ACTIVITY_OTHER'] if 'CA_ACTIVITY_OTHER' in doc['FORM_SECTION']['F02_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CONTRACT_TITLE'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['TITLE']['P']
        d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['REFERENCE_NUMBER'] if 'REFERENCE_NUMBER' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
        d['CPV_MAIN'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['CPV_MAIN']['CPV_CODE']['@CODE']
        d['TYPE_CONTRACT'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['TYPE_CONTRACT']['@CTYPE']   
        shrt_dsc = []
        if isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'], list):
            for each in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P']:
                if isinstance(each, dict):
                    shrt_dsc.append(each['#text'])
                else:
                    shrt_dsc.append(each)
        elif isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'], dict):
            shrt_dsc.append(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P']['#text'])
        else:
            shrt_dsc.append(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'])
        d['SHORT_DESCR'] = ','.join([str(x) for x in shrt_dsc])
        if 'NO_LOT_DIVISION' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT'].keys():
            d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['NO_LOT_DIVISION']
        if 'LOT_DIVISION' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT'].keys():
            d['MAX_LOT_DIVISION'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_NUMBER'] if 'LOT_MAX_NUMBER' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
            d['MAX_LOT_PER_TENDERER'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'] if 'LOT_MAX_ONE_TENDERER' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
            d['LOT_ALL'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_ALL'] if 'LOT_ALL' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
        if isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], dict):
            if 'MAIN_SITE' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                    main_site = []
                    for item in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']:
                        if isinstance(item, dict):
                            main_site.append(item['#text'])
                        elif isinstance(item, str):
                            main_site.append(item)
                        else:
                            main_site.append(str(item))
                    d['MAIN_SITE'] = ', '.join(main_site)
                elif isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']['#text']
                else:
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']
            if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys() else ''
                else:
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']]) 
            if isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
            else:
                d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
            d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([str(s['#text']) if isinstance(s, dict) else str(s) for s in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']])
            if 'DURATION' in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']
        elif isinstance(doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
            cpv_add, n2016_nuts, descr, short_descr, duration, main_site = [], [], [], [], [], []
            for each in doc['FORM_SECTION']['F02_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']:
                if 'MAIN_SITE' in each.keys():
                    if isinstance(each['MAIN_SITE']['P'], list):
                        for item in each['MAIN_SITE']['P']:
                            if isinstance(item, dict):
                                main_site.append(item['#text'])
                            elif isinstance(item, str):
                                main_site.append(item)
                            else:
                                main_site.append(str(item))
                    elif isinstance(each['MAIN_SITE']['P'], dict):
                        main_site.append(each['MAIN_SITE']['P']['#text'])
                    else:
                        main_site.append(each['MAIN_SITE']['P'])
                if 'CPV_ADDITIONAL' in each.keys():
                    if isinstance(each['CPV_ADDITIONAL'], dict):
                        cpv_add.append(each['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                    else:
                        for item in each['CPV_ADDITIONAL']:
                            cpv_add.append(item['CPV_CODE']['@CODE'])
                if 'n2016:NUTS' in each.keys():
                    if isinstance(each['n2016:NUTS'], dict):
                        n2016_nuts.append(each['n2016:NUTS']['@CODE'])
                    else:
                        n2016_nuts.append(','.join(item['@CODE'] for item in each['n2016:NUTS']))
                if 'SHORT_DESCR' in each.keys():
                    if isinstance(each['SHORT_DESCR'], dict):
                        if isinstance(each['SHORT_DESCR']['P'], dict):
                            short_descr.append(each['SHORT_DESCR']['P']['#text'])
                        else:
                            if each['SHORT_DESCR']['P'] is not None:
                                short_descr.append(each['SHORT_DESCR']['P'])
                if 'DURATION' in each.keys():
                    if isinstance(each['DURATION'], dict):
                        duration.append(each['DURATION']['#text']+each['DURATION']['@TYPE'])
                    else:
                        for item in each['DURATION']:
                            duration.append(item['#text']+item['@TYPE'])
            for desc in short_descr:
                if isinstance(desc, list):
                    for txt in desc:
                        descr.append(txt)
                else:
                    descr.append(desc)
            d['MAIN_SITE'] = ';'.join([str(x) if x is None else x for x in main_site])
            d['OBJECT_DESCR_CPV_ADDITIONAL'] = ';'.join(cpv_add)
            d['OBJECT_DESCR_n2016-NUTS'] = ';'.join(n2016_nuts)
            d['OBJECT_DESCR_SHORT_DESCR'] = ';'.join([str(i) for i in descr])
            d['OBJECT_DESCR_DURATION'] = ';'.join(duration)
        d['PROCEDURE_DATE_RECEIPT_TENDERS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['DATE_RECEIPT_TENDERS'] if 'DATE_RECEIPT_TENDERS' in doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE'].keys() else ''
        d['PROCEDURE_TIME_RECEIPT_TENDERS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['TIME_RECEIPT_TENDERS'] if 'TIME_RECEIPT_TENDERS' in doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE'].keys() else ''
        d['PROCEDURE_DURATION_TENDER_VALID'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['DURATION_TENDER_VALID']['#text'] + ' ' + doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['DURATION_TENDER_VALID']['@TYPE'] if 'DURATION_TENDER_VALID' in doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE'].keys() else ''
        if 'OPENING_CONDITION' in doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE'].keys():
            d['PROCEDURE_DATE_OPENING_TENDERS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['OPENING_CONDITION']['DATE_OPENING_TENDERS'] if 'DATE_OPENING_TENDERS' in doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['OPENING_CONDITION'].keys() else ''
            d['PROCEDURE_TIME_OPENING_TENDERS'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['OPENING_CONDITION']['TIME_OPENING_TENDERS'] if 'TIME_OPENING_TENDERS' in doc['FORM_SECTION']['F02_2014'][EN_POS]['PROCEDURE']['OPENING_CONDITION'].keys() else ''
        d['DATE_DISPATCH_NOTICE'] = doc['FORM_SECTION']['F02_2014'][EN_POS]['COMPLEMENTARY_INFO']['DATE_DISPATCH_NOTICE'] if 'DATE_DISPATCH_NOTICE' in doc['FORM_SECTION']['F02_2014'][EN_POS]['COMPLEMENTARY_INFO'].keys() else ''
    
    elif isinstance(doc['FORM_SECTION']['F02_2014'], dict):
        d['FORM'] = doc['FORM_SECTION']['F02_2014']['@FORM']
        d['LEGAL_BASIS'] = doc['FORM_SECTION']['F02_2014']['LEGAL_BASIS']['@VALUE'] if 'LEGAL_BASIS' in doc['FORM_SECTION']['F02_2014'].keys() else ''
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['n2016-NUTS'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['n2016:NUTS']['@CODE']
        d['CA_TYPE'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['CA_TYPE']['@VALUE'] if 'CA_TYPE' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['CA_TYPE_OTHER'] if 'CA_TYPE_OTHER' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY']['CA_ACTIVITY_OTHER'] if 'CA_ACTIVITY_OTHER' in doc['FORM_SECTION']['F02_2014']['CONTRACTING_BODY'].keys() else ''
        d['CONTRACT_TITLE'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['TITLE']['P']
        d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['REFERENCE_NUMBER'] if 'REFERENCE_NUMBER' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT'].keys() else ''
        d['CPV_MAIN'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['CPV_MAIN']['CPV_CODE']['@CODE']
        d['TYPE_CONTRACT'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['TYPE_CONTRACT']['@CTYPE']
        shrt_dsc = []
        if isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'], list):
            for each in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P']:
                if isinstance(each, dict):
                    shrt_dsc.append(each['#text'])
                else:
                    shrt_dsc.append(each)
        elif isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'], dict):
            shrt_dsc.append(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P']['#text'])
        else:
            shrt_dsc.append(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'])
        d['SHORT_DESCR'] = ','.join([str(x) for x in shrt_dsc])
        if 'NO_LOT_DIVISION' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT'].keys():
            d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['NO_LOT_DIVISION']
        if 'LOT_DIVISION' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT'].keys():
            d['MAX_LOT_DIVISION'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_NUMBER'] if 'LOT_MAX_NUMBER' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
            d['MAX_LOT_PER_TENDERER'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'] if 'LOT_MAX_ONE_TENDERER' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
            d['LOT_ALL'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_ALL'] if 'LOT_ALL' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
        if isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], dict):
            if 'MAIN_SITE' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                    main_site = []
                    for item in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']:
                        if isinstance(item, dict):
                            main_site.append(item['#text'])
                        elif isinstance(item, str):
                            main_site.append(item)
                        else:
                            main_site.append(str(item))
                    d['MAIN_SITE'] = ', '.join(main_site)
                elif isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']['#text']
                else:
                    d['MAIN_SITE'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']
            if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                if isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE']
                else:
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']])
            if isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
            else:
                d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
            
            obj_shrt_dsc = []
            if doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P'] is not None:
                for item in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']:
                    if isinstance(item, dict):
                        obj_shrt_dsc.append(item['#text'])
            d['OBJECT_DESCR_SHORT_DESCR'] = ','.join(obj_shrt_dsc)
            if 'DURATION' in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']
        elif isinstance(doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
            cpv_add, n2016_nuts, descr, short_descr, duration, main_site = [], [], [], [], [], []
            for each in doc['FORM_SECTION']['F02_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']:
                if 'MAIN_SITE' in each.keys():
                    if isinstance(each['MAIN_SITE']['P'], list):
                        for item in each['MAIN_SITE']['P']:
                            if isinstance(item, dict):
                                main_site.append(item['#text'])
                            elif isinstance(item, str):
                                main_site.append(item)
                            else:
                                main_site.append(str(item))
                    elif isinstance(each['MAIN_SITE']['P'], dict):
                        main_site.append(each['MAIN_SITE']['P']['#text'])
                    else:
                        main_site.append(each['MAIN_SITE']['P'])
                if 'CPV_ADDITIONAL' in each.keys():
                    if isinstance(each['CPV_ADDITIONAL'], dict):
                        cpv_add.append(each['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                    else:
                        for item in each['CPV_ADDITIONAL']:
                            cpv_add.append(item['CPV_CODE']['@CODE'])
                if 'n2016:NUTS' in each.keys():
                    if isinstance(each['n2016:NUTS'], dict):
                        n2016_nuts.append(each['n2016:NUTS']['@CODE'])
                    else:
                        n2016_nuts.append(','.join(item['@CODE'] for item in each['n2016:NUTS']))
                if 'SHORT_DESCR' in each.keys():
                    if isinstance(each['SHORT_DESCR'], dict):
                        if isinstance(each['SHORT_DESCR']['P'], dict):
                            short_descr.append(each['SHORT_DESCR']['P']['#text'])
                        else:
                            if each['SHORT_DESCR']['P'] is not None:
                                short_descr.append(each['SHORT_DESCR']['P'])
                if 'DURATION' in each.keys():
                    if isinstance(each['DURATION'], dict):
                        duration.append(each['DURATION']['#text']+each['DURATION']['@TYPE'])
                    else:
                        for item in each['DURATION']:
                            duration.append(item['#text']+item['@TYPE'])
            for desc in short_descr:
                if isinstance(desc, list):
                    for txt in desc:
                        descr.append(txt)
                else:
                    descr.append(desc)
            d['MAIN_SITE'] = ';'.join([str(x) if x is None else x for x in main_site])
            d['OBJECT_DESCR_CPV_ADDITIONAL'] = ';'.join(cpv_add)
            d['OBJECT_DESCR_n2016-NUTS'] = ';'.join(n2016_nuts)
            d['OBJECT_DESCR_SHORT_DESCR'] = ';'.join([str(i) for i in descr])
            d['OBJECT_DESCR_DURATION'] = ';'.join(duration)
        d['PROCEDURE_DATE_RECEIPT_TENDERS'] = doc['FORM_SECTION']['F02_2014']['PROCEDURE']['DATE_RECEIPT_TENDERS'] if 'DATE_RECEIPT_TENDERS' in doc['FORM_SECTION']['F02_2014']['PROCEDURE'].keys() else ''
        d['PROCEDURE_TIME_RECEIPT_TENDERS'] = doc['FORM_SECTION']['F02_2014']['PROCEDURE']['TIME_RECEIPT_TENDERS'] if 'TIME_RECEIPT_TENDERS' in doc['FORM_SECTION']['F02_2014']['PROCEDURE'].keys() else ''
        d['PROCEDURE_DURATION_TENDER_VALID'] = doc['FORM_SECTION']['F02_2014']['PROCEDURE']['DURATION_TENDER_VALID']['#text'] + ' ' + doc['FORM_SECTION']['F02_2014']['PROCEDURE']['DURATION_TENDER_VALID']['@TYPE'] if 'DURATION_TENDER_VALID' in doc['FORM_SECTION']['F02_2014']['PROCEDURE'].keys() else ''
        if 'OPENING_CONDITION' in doc['FORM_SECTION']['F02_2014']['PROCEDURE'].keys():
            d['PROCEDURE_DATE_OPENING_TENDERS'] = doc['FORM_SECTION']['F02_2014']['PROCEDURE']['OPENING_CONDITION']['DATE_OPENING_TENDERS'] if 'DATE_OPENING_TENDERS' in doc['FORM_SECTION']['F02_2014']['PROCEDURE']['OPENING_CONDITION'].keys() else ''
            d['PROCEDURE_TIME_OPENING_TENDERS'] = doc['FORM_SECTION']['F02_2014']['PROCEDURE']['OPENING_CONDITION']['TIME_OPENING_TENDERS'] if 'TIME_OPENING_TENDERS' in doc['FORM_SECTION']['F02_2014']['PROCEDURE']['OPENING_CONDITION'].keys() else ''
        d['DATE_DISPATCH_NOTICE'] = doc['FORM_SECTION']['F02_2014']['COMPLEMENTARY_INFO']['DATE_DISPATCH_NOTICE'] if 'DATE_DISPATCH_NOTICE' in doc['FORM_SECTION']['F02_2014']['COMPLEMENTARY_INFO'].keys() else ''
    
    return d

def R209_PIN(doc, EN_POS):
    d = dict()
    if '@VERSION' in doc.keys():
        d['VERSION'] = doc['@VERSION']
    else:
        if isinstance(doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]], dict):
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]]['@VERSION']
        else:
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]][0]['@VERSION']
    if isinstance(doc['FORM_SECTION']['F01_2014'], list):
        d['FORM'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['@FORM']
        d['LEGAL_BASIS'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['LEGAL_BASIS']['@VALUE']
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['n2016-NUTS'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['n2016:NUTS']['@CODE']
        d['CA_TYPE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['CA_TYPE']['@VALUE'] if 'CA_TYPE' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['CA_TYPE_OTHER'] if 'CA_TYPE_OTHER' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY']['CA_ACTIVITY_OTHER'] if 'CA_ACTIVITY_OTHER' in doc['FORM_SECTION']['F01_2014'][EN_POS]['CONTRACTING_BODY'].keys() else ''
        d['CONTRACT_COVERED_GPA'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['PROCEDURE']['CONTRACT_COVERED_GPA'] if 'CONTRACT_COVERED_GPA' in doc['FORM_SECTION']['F01_2014'][EN_POS]['PROCEDURE'].keys() else ''
        d['DATE_DISPATCH_NOTICE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['COMPLEMENTARY_INFO']['DATE_DISPATCH_NOTICE'] if 'DATE_DISPATCH_NOTICE' in doc['FORM_SECTION']['F01_2014'][EN_POS]['COMPLEMENTARY_INFO'].keys() else ''
        if isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'], dict):
            d['TITLE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['TITLE']['P']
            d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['REFERENCE_NUMBER'] if 'REFERENCE_NUMBER' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
            d['CPV_MAIN'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['CPV_MAIN']['CPV_CODE']['@CODE']
            d['TYPE_CONTRACT'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['TYPE_CONTRACT']['@CTYPE']
            d['TOTAL_VAL_CURR'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['VAL_TOTAL']['@CURRENCY'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
            d['TOTAL_VAL'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['VAL_TOTAL']['#text'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
            shrt_dsc = []
            if isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'], list):
                for each in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P']:
                    if isinstance(each, dict):
                        shrt_dsc.append(each['#text'])
                    else:
                        shrt_dsc.append(each)
            elif isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'], dict):
                shrt_dsc.append(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P']['#text'])
            else:
                shrt_dsc.append(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['SHORT_DESCR']['P'])
            d['SHORT_DESCR'] = ','.join([str(x) for x in shrt_dsc])
            if 'NO_LOT_DIVISION' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'].keys():
                d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['NO_LOT_DIVISION']
            if 'LOT_DIVISION' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'].keys():
                if doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'] is not None:
                    d['MAX_LOT_DIVISION'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_NUMBER'] if 'LOT_MAX_NUMBER' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                    d['MAX_LOT_PER_TENDERER'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'] if 'LOT_MAX_ONE_TENDERER' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                    d['LOT_ALL'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_ALL'] if 'LOT_ALL' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
            if isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], dict):
                if 'MAIN_SITE' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                    if isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                        main_site = []
                        for item in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']:
                            if isinstance(item, dict):
                                main_site.append(item['#text'])
                            elif isinstance(item, str):
                                main_site.append(item)
                            else:
                                main_site.append(str(item))
                        d['MAIN_SITE'] = ','.join(main_site)
                    elif isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                        d['MAIN_SITE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']['#text']
                    else:
                        d['MAIN_SITE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']
                if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                    if isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                        d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys() else ''
                    else:
                        d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']])
                if isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                    d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
                else:
                    d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
                    d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([s['#text'] if isinstance(s, dict) else s for s in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']])
                    if 'DURATION' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                        d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']
            elif isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
                cpv_add_lst, n2016_nuts_lst, short_dsc_lst, durr_lst, main_site_lst = [],[],[],[],[]
                for each in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['OBJECT_DESCR']:
                    if 'MAIN_SITE' in each.keys():
                        if isinstance(each['MAIN_SITE']['P'], list):
                            for item in each['MAIN_SITE']['P']:
                                if isinstance(item, dict):
                                    main_site_lst.append(item['#text'])
                                elif isinstance(item, str):
                                    main_site_lst.append(item)
                                else:
                                    main_site_lst.append(str(item))
                        elif isinstance(each['MAIN_SITE']['P'], dict):
                            main_site_lst.append(each['MAIN_SITE']['P']['#text'])
                        else:
                            main_site_lst.append(each['MAIN_SITE']['P'])
                    if 'CPV_ADDITIONAL' in each.keys():
                        if isinstance(each['CPV_ADDITIONAL'], dict):
                            cpv_add_lst.append(each['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                        else:
                            cpv_add_lst.append(','.join([item['CPV_CODE']['@CODE'] for item in each['CPV_ADDITIONAL']]))
                    if isinstance(each['n2016:NUTS'], dict):
                        n2016_nuts_lst.append(each['n2016:NUTS']['@CODE'])
                    else:
                        n2016_nuts_lst.append(','.join([item['@CODE'] for item in each['n2016:NUTS']]))
                    if isinstance(each['SHORT_DESCR']['P'], list):
                        for item in each['SHORT_DESCR']['P']:
                            if isinstance(item, dict):
                                short_dsc_lst.append(item['#text'])
                            else:
                                short_dsc_lst.append(item)
                    else:
                        short_dsc_lst.append(each['SHORT_DESCR']['P'])
                    if 'DURATION' in each.keys():
                        durr_lst.append(each['DURATION']['#text']+' '+each['DURATION']['@TYPE'])
                d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site_lst])
                d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_add_lst)
                d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(n2016_nuts_lst)
                d['OBJECT_DESCR_SHORT_DESCR'] = '; '.join(short_dsc_lst)
                d['OBJECT_DESCR_DURATION'] = '; '.join(durr_lst)
            d['DATE_PUBLICATION_NOTICE'] = doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']['DATE_PUBLICATION_NOTICE'] if 'DATE_PUBLICATION_NOTICE' in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'].keys() else ''
        
        elif isinstance(doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT'], list):
            titles, cpv_mains, cnt_types, shrt_dsc, val_est_tot_cur, val_est_tot = [],[],[],[],[],[]
            tot_val_cur, tot_val, no_lot_div, max_lot, max_lot_tend, lot_all = [],[],[],[],[],[]
            cpv_add, n2016_nuts, obj_dsc, durations, pub_not_date, main_site = [],[],[],[],[],[]
            for each in doc['FORM_SECTION']['F01_2014'][EN_POS]['OBJECT_CONTRACT']:
                if 'TITLE' in each.keys():
                    titles.append(each['TITLE']['P'])
                if 'CPV_MAIN' in each.keys():
                    cpv_mains.append(each['CPV_MAIN']['CPV_CODE']['@CODE'])
                if 'TYPE_CONTRACT' in each.keys():
                    cnt_types.append(each['TYPE_CONTRACT']['@CTYPE'])
                if 'SHORT_DESCR' in each.keys():
                    if isinstance(each['SHORT_DESCR'], dict):
                        if isinstance(each['SHORT_DESCR']['P'], dict):
                            shrt_dsc.append(each['SHORT_DESCR']['P']['#text'])
                        else:
                            if isinstance(each['SHORT_DESCR']['P'], str):
                                shrt_dsc.append(each['SHORT_DESCR']['P'])
                            elif isinstance(each['SHORT_DESCR']['P'], list):
                                for s in each['SHORT_DESCR']['P']:
                                    if isinstance(s, str):
                                        shrt_dsc.append(s)
                                    else:
                                        shrt_dsc.append(s['#text'])
                    else:
                        for item in each['SHORT_DESCR']['P']:
                            if isinstance(item, str):
                                shrt_dsc.append(item)
                if 'VAL_ESTIMATED_TOTAL' in each.keys():
                    val_est_tot_cur.append(each['VAL_ESTIMATED_TOTAL']['@CURRENCY'])
                    val_est_tot.append(each['VAL_ESTIMATED_TOTAL']['#text'])
                if 'VAL_TOTAL' in each.keys():
                    tot_val_cur.append(each['VAL_TOTAL']['@CURRENCY'])
                    tot_val.append(each['VAL_TOTAL']['#text'])
                if 'NO_LOT_DIVISION' in each.keys():
                    no_lot_div.append(each['NO_LOT_DIVISION'])
                if 'LOT_DIVISION' in each.keys():
                    if each['LOT_DIVISION'] is not None:
                        if 'LOT_MAX_NUMBER' in each['LOT_DIVISION'].keys():
                            max_lot.append(each['LOT_DIVISION']['LOT_MAX_NUMBER'])
                        if 'LOT_MAX_ONE_TENDERER' in each['LOT_DIVISION'].keys():
                            max_lot_tend.append(each['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'])
                        if 'LOT_ALL' in each['LOT_DIVISION'].keys():
                            lot_all.append(each['LOT_DIVISION']['LOT_ALL'])
                if 'OBJECT_DESCR' in each.keys():
                    if isinstance(each['OBJECT_DESCR'], dict):
                        if 'MAIN_SITE' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                                for item in each['OBJECT_DESCR']['MAIN_SITE']['P']:
                                    if isinstance(item, dict):
                                        main_site.append(item['#text'])
                                    elif isinstance(item, str):
                                        main_site.append(item)
                                    else:
                                        main_site.append(str(item))
                            elif isinstance(each['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                                main_site.append(each['OBJECT_DESCR']['MAIN_SITE']['P']['#text'])
                            else:
                                main_site.append(each['OBJECT_DESCR']['MAIN_SITE']['P'])
                        if 'CPV_ADDITIONAL' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                                cpv_add.append(each['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                            else:
                                cpv_add.append(','.join([item['CPV_CODE']['@CODE'] for item in each['OBJECT_DESCR']['CPV_ADDITIONAL']]))
                        if 'n2016:NUTS' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['n2016:NUTS'], dict):
                                n2016_nuts.append(each['OBJECT_DESCR']['n2016:NUTS']['@CODE'])
                            else:
                                n2016_nuts.append(','.join([item['@CODE'] for item in each['OBJECT_DESCR']['n2016:NUTS']]))
                        if 'SHORT_DESCR' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['SHORT_DESCR'], dict):
                                if isinstance(each['OBJECT_DESCR']['SHORT_DESCR']['P'], dict):
                                    obj_dsc.append(each['OBJECT_DESCR']['SHORT_DESCR']['P']['#text'])
                                else:
                                    if isinstance(each['OBJECT_DESCR']['SHORT_DESCR']['P'], str):
                                        obj_dsc.append(each['OBJECT_DESCR']['SHORT_DESCR']['P'])
                                    elif isinstance(each['OBJECT_DESCR']['SHORT_DESCR']['P'], list):
                                        for s in each['OBJECT_DESCR']['SHORT_DESCR']['P']:
                                            if isinstance(s, str):
                                                obj_dsc.append(s)
                                            else:
                                                obj_dsc.append(s['#text'])
                            else:
                                obj_dsc.append(''.join([item for item in each['OBJECT_DESCR']['SHORT_DESCR']['P']]))
                        if 'DURATION' in each['OBJECT_DESCR'].keys():
                            durations.append(each['OBJECT_DESCR']['DURATION']['#text']+' '+each['OBJECT_DESCR']['DURATION']['@TYPE'])
                    elif isinstance(each['OBJECT_DESCR'], list):
                        for item in each['OBJECT_DESCR']:
                            if 'MAIN_SITE' in item.keys():
                                if isinstance(item['MAIN_SITE']['P'], list):
                                    for ii in item['MAIN_SITE']['P']:
                                        if isinstance(item, dict):
                                            main_site.append(item['#text'])
                                        elif isinstance(item, str):
                                            main_site.append(item)
                                        else:
                                            main_site.append(str(item))
                                elif isinstance(item['MAIN_SITE']['P'], dict):
                                    main_site.append(item['MAIN_SITE']['P']['#text'])
                                else:
                                    main_site.append(item['MAIN_SITE']['P'])
                            if 'CPV_ADDITIONAL' in item.keys():
                                if isinstance(item['CPV_ADDITIONAL'], dict):
                                    cpv_add.append(item['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                                else:
                                    cpv_add.append(','.join([ii['CPV_CODE']['@CODE'] for ii in item['CPV_ADDITIONAL']]))
                            if 'n2016:NUTS' in item.keys():
                                if isinstance(item['n2016:NUTS'], dict):
                                    n2016_nuts.append(item['n2016:NUTS']['@CODE'])
                                else:
                                    n2016_nuts.append(','.join([item['@CODE'] for item in item['n2016:NUTS']]))
                            if 'SHORT_DESCR' in item.keys():
                                if isinstance(item['SHORT_DESCR'], dict):
                                    if isinstance(item['SHORT_DESCR']['P'], dict):
                                        obj_dsc.append(item['SHORT_DESCR']['P']['#text'])
                                    else:
                                        if isinstance(item['SHORT_DESCR']['P'], str):
                                            obj_dsc.append(item['SHORT_DESCR']['P'])
                                        elif isinstance(item['SHORT_DESCR']['P'], list):
                                            for s in item['SHORT_DESCR']['P']:
                                                if isinstance(s, str):
                                                    obj_dsc.append(s)
                                                else:
                                                    obj_dsc.append(s['#text'])
                                else:
                                    obj_dsc.append(''.join([item for item in each['OBJECT_DESCR']['SHORT_DESCR']['P']]))
                            if 'DURATION' in item.keys():
                                durations.append(item['DURATION']['#text']+' '+item['DURATION']['@TYPE'])
                if 'DATE_PUBLICATION_NOTICE' in each.keys():
                    pub_not_date.append(each['DATE_PUBLICATION_NOTICE'])       
            
            d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site])
            d['TITLE'] = '; '.join(titles)
            d['CPV_MAIN'] = '; '.join(cpv_mains)
            d['TYPE_CONTRACT'] = '; '.join(cnt_types)
            d['SHORT_DESCR'] = '; '.join(shrt_dsc)
            d['ESTIMATED_TOTAL_VALUE_CURR'] = '; '.join(val_est_tot_cur)
            d['ESTIMATED_TOTAL_VALUE'] = '; '.join(val_est_tot)
            d['NO_LOT_DIVISION'] = '; '.join([str(x) for x in no_lot_div])
            d['MAX_LOT_DIVISION'] = '; '.join([str(x) for x in max_lot])
            d['MAX_LOT_PER_TENDERER'] = '; '.join([str(x) for x in max_lot_tend])
            d['LOT_ALL'] = '; '.join([str(x) for x in lot_all])
            d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_add)
            d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(n2016_nuts)
            d['OBJECT_DESCR_SHORT_DESCR'] = '; '.join(obj_dsc)
            d['DURATION'] = '; '.join(durations)
            d['DATE_PUBLICATION_NOTICE'] = '; '.join(pub_not_date)

    elif isinstance(doc['FORM_SECTION']['F01_2014'], dict):
        d['FORM'] = doc['FORM_SECTION']['F01_2014']['@FORM']
        d['LEGAL_BASIS'] = doc['FORM_SECTION']['F01_2014']['LEGAL_BASIS']['@VALUE']
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY'].keys() else ''
        d['n2016-NUTS'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['ADDRESS_CONTRACTING_BODY']['n2016:NUTS']['@CODE']
        d['CA_TYPE'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['CA_TYPE']['@VALUE'] if 'CA_TYPE' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['CA_TYPE_OTHER'] if 'CA_TYPE_OTHER' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['CA_ACTIVITY']['@VALUE'] if 'CA_ACTIVITY' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY'].keys() else ''
        d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY']['CA_ACTIVITY_OTHER'] if 'CA_ACTIVITY_OTHER' in doc['FORM_SECTION']['F01_2014']['CONTRACTING_BODY'].keys() else ''
        d['CONTRACT_COVERED_GPA'] = doc['FORM_SECTION']['F01_2014']['PROCEDURE']['CONTRACT_COVERED_GPA'] if 'CONTRACT_COVERED_GPA' in doc['FORM_SECTION']['F01_2014']['PROCEDURE'].keys() else ''
        d['DATE_DISPATCH_NOTICE'] = doc['FORM_SECTION']['F01_2014']['COMPLEMENTARY_INFO']['DATE_DISPATCH_NOTICE'] if 'DATE_DISPATCH_NOTICE' in doc['FORM_SECTION']['F01_2014']['COMPLEMENTARY_INFO'].keys() else ''
        if isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'], dict):
            d['TITLE'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['TITLE']['P']
            d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['REFERENCE_NUMBER'] if 'REFERENCE_NUMBER' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'].keys() else ''
            d['CPV_MAIN'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['CPV_MAIN']['CPV_CODE']['@CODE']
            d['TYPE_CONTRACT'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['TYPE_CONTRACT']['@CTYPE']
            d['TOTAL_VAL_CURR'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['VAL_TOTAL']['@CURRENCY'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'].keys() else ''
            d['TOTAL_VAL'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['VAL_TOTAL']['#text'] if 'VAL_TOTAL' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'].keys() else ''
            shrt_dsc = []
            if isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'], list):
                for each in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P']:
                    if isinstance(each, dict):
                        shrt_dsc.append(each['#text'])
                    else:
                        shrt_dsc.append(each)
            elif isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'], dict):
                shrt_dsc.append(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P']['#text'])
            else:
                shrt_dsc.append(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['SHORT_DESCR']['P'])
            d['SHORT_DESCR'] = ' '.join([str(x) for x in shrt_dsc])
            if 'NO_LOT_DIVISION' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'].keys():
                d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['NO_LOT_DIVISION']
            if 'LOT_DIVISION' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'].keys():
                if doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION'] is not None:
                    d['MAX_LOT_DIVISION'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_NUMBER'] if 'LOT_MAX_NUMBER' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                    d['MAX_LOT_PER_TENDERER'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'] if 'LOT_MAX_ONE_TENDERER' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
                    d['LOT_ALL'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION']['LOT_ALL'] if 'LOT_ALL' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['LOT_DIVISION'].keys() else ''
            
            if isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], dict):
                if 'MAIN_SITE' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                    if isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                        main_site = []
                        for item in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']:
                            if isinstance(item, dict):
                                main_site.append(item['#text'])
                            elif isinstance(item, str):
                                main_site.append(item)
                            else:
                                main_site.append(str(item))
                        d['MAIN_SITE'] = ','.join(main_site)
                    elif isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                        d['MAIN_SITE'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']['#text']
                    else:
                        d['MAIN_SITE'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['MAIN_SITE']['P']
                if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                    if isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                        d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'] if 'CPV_ADDITIONAL' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys() else ''
                    else:
                        d['OBJECT_DESCR_CPV_ADDITIONAL'] = ','.join([item['CPV_CODE']['@CODE'] for item in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['CPV_ADDITIONAL']])
                if isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'], dict):
                    d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS']['@CODE']
                else:
                    d['OBJECT_DESCR_n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['n2016:NUTS'])
                    d['OBJECT_DESCR_SHORT_DESCR'] = ''.join([s['#text'] if isinstance(s, dict) else s for s in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['SHORT_DESCR']['P']])
                    if 'DURATION' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'].keys():
                        d['OBJECT_DESCR_DURATION'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['#text'] + ' ' + doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']['DURATION']['@TYPE']
            
            elif isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR'], list):
                cpv_add_lst, n2016_nuts_lst, short_dsc_lst, durr_lst, main_site_lst = [],[],[],[],[]
                for each in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['OBJECT_DESCR']:
                    if 'MAIN_SITE' in each.keys():
                        if isinstance(each['MAIN_SITE']['P'], list):
                            for item in each['MAIN_SITE']['P']:
                                if isinstance(item, dict):
                                    main_site_lst.append(item['#text'])
                                elif isinstance(item, str):
                                    main_site_lst.append(item)
                                else:
                                    main_site_lst.append(str(item))
                        elif isinstance(each['MAIN_SITE']['P'], dict):
                            main_site_lst.append(each['MAIN_SITE']['P']['#text'])
                        else:
                            main_site_lst.append(each['MAIN_SITE']['P'])
                    if 'CPV_ADDITIONAL' in each.keys():
                        if isinstance(each['CPV_ADDITIONAL'], dict):
                            cpv_add_lst.append(each['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                        else:
                            cpv_add_lst.append(','.join([item['CPV_CODE']['@CODE'] for item in each['CPV_ADDITIONAL']]))
                    if isinstance(each['n2016:NUTS'], dict):
                        n2016_nuts_lst.append(each['n2016:NUTS']['@CODE'])
                    else:
                        n2016_nuts_lst.append(','.join([item['@CODE'] for item in each['n2016:NUTS']]))
                    if isinstance(each['SHORT_DESCR']['P'], list):
                        for item in each['SHORT_DESCR']['P']:
                            if isinstance(item, dict):
                                short_dsc_lst.append(item['#text'])
                            else:
                                short_dsc_lst.append(item)
                    else:
                        if isinstance(each['SHORT_DESCR']['P'], dict):
                            short_dsc_lst.append(each['SHORT_DESCR']['P']['#text'])
                        else:
                            short_dsc_lst.append(each['SHORT_DESCR']['P'])
                    if 'DURATION' in each.keys():
                        durr_lst.append(each['DURATION']['#text']+' '+each['DURATION']['@TYPE'])
                d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site_lst])
                d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_add_lst)
                d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(n2016_nuts_lst)
                d['OBJECT_DESCR_SHORT_DESCR'] = '; '.join(short_dsc_lst)
                d['OBJECT_DESCR_DURATION'] = '; '.join(durr_lst)
            d['DATE_PUBLICATION_NOTICE'] = doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']['DATE_PUBLICATION_NOTICE'] if 'DATE_PUBLICATION_NOTICE' in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'].keys() else ''

        elif isinstance(doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT'], list):
            titles, cpv_mains, cnt_types, shrt_dsc, val_est_tot_cur, val_est_tot = [],[],[],[],[],[]
            tot_val_cur, tot_val, no_lot_div, max_lot, max_lot_tend, lot_all = [],[],[],[],[],[]
            cpv_add, n2016_nuts, obj_dsc, durations, pub_not_date, main_site = [],[],[],[],[],[]
            for each in doc['FORM_SECTION']['F01_2014']['OBJECT_CONTRACT']:
                if 'TITLE' in each.keys():
                    titles.append(each['TITLE']['P'])
                if 'CPV_MAIN' in each.keys():
                    cpv_mains.append(each['CPV_MAIN']['CPV_CODE']['@CODE'])
                if 'TYPE_CONTRACT' in each.keys():
                    cnt_types.append(each['TYPE_CONTRACT']['@CTYPE'])
                if 'SHORT_DESCR' in each.keys():
                    if isinstance(each['SHORT_DESCR'], dict):
                        if isinstance(each['SHORT_DESCR']['P'], dict):
                            shrt_dsc.append(each['SHORT_DESCR']['P']['#text'])
                        else:
                            if isinstance(each['SHORT_DESCR']['P'], str):
                                shrt_dsc.append(each['SHORT_DESCR']['P'])
                            elif isinstance(each['SHORT_DESCR']['P'], list):
                                for s in each['SHORT_DESCR']['P']:
                                    if isinstance(s, str):
                                        shrt_dsc.append(s)
                                    else:
                                        shrt_dsc.append(s['#text'])
                    else:
                        for item in each['SHORT_DESCR']['P']:
                            if isinstance(item, str):
                                shrt_dsc.append(item)
                if 'VAL_ESTIMATED_TOTAL' in each.keys():
                    val_est_tot_cur.append(each['VAL_ESTIMATED_TOTAL']['@CURRENCY'])
                    val_est_tot.append(each['VAL_ESTIMATED_TOTAL']['#text'])
                if 'VAL_TOTAL' in each.keys():
                    tot_val_cur.append(each['VAL_TOTAL']['@CURRENCY'])
                    tot_val.append(each['VAL_TOTAL']['#text'])
                if 'NO_LOT_DIVISION' in each.keys():
                    no_lot_div.append(each['NO_LOT_DIVISION'])
                if 'LOT_DIVISION' in each.keys():
                    if each['LOT_DIVISION'] is not None:
                        if 'LOT_MAX_NUMBER' in each['LOT_DIVISION'].keys():
                            max_lot.append(each['LOT_DIVISION']['LOT_MAX_NUMBER'])
                        if 'LOT_MAX_ONE_TENDERER' in each['LOT_DIVISION'].keys():
                            max_lot_tend.append(each['LOT_DIVISION']['LOT_MAX_ONE_TENDERER'])
                        if 'LOT_ALL' in each['LOT_DIVISION'].keys():
                            lot_all.append(each['LOT_DIVISION']['LOT_ALL'])
                if 'OBJECT_DESCR' in each.keys():
                    if isinstance(each['OBJECT_DESCR'], dict):
                        if 'MAIN_SITE' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['MAIN_SITE']['P'], list):
                                for item in each['OBJECT_DESCR']['MAIN_SITE']['P']:
                                    if isinstance(item, dict):
                                        main_site.append(item['#text'])
                                    elif isinstance(item, str):
                                        main_site.append(item)
                                    else:
                                        main_site.append(str(item))
                            elif isinstance(each['OBJECT_DESCR']['MAIN_SITE']['P'], dict):
                                main_site.append(each['OBJECT_DESCR']['MAIN_SITE']['P']['#text'])
                            else:
                                main_site.append(each['OBJECT_DESCR']['MAIN_SITE']['P'])
                        if 'CPV_ADDITIONAL' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['CPV_ADDITIONAL'], dict):
                                cpv_add.append(each['OBJECT_DESCR']['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                            else:
                                cpv_add.append(','.join([item['CPV_CODE']['@CODE'] for item in each['OBJECT_DESCR']['CPV_ADDITIONAL']]))
                        if 'n2016:NUTS' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['n2016:NUTS'], dict):
                                n2016_nuts.append(each['OBJECT_DESCR']['n2016:NUTS']['@CODE'])
                            else:
                                n2016_nuts.append(','.join([item['@CODE'] for item in each['OBJECT_DESCR']['n2016:NUTS']]))
                        if 'SHORT_DESCR' in each['OBJECT_DESCR'].keys():
                            if isinstance(each['OBJECT_DESCR']['SHORT_DESCR'], dict):
                                if isinstance(each['OBJECT_DESCR']['SHORT_DESCR']['P'], dict):
                                    obj_dsc.append(each['OBJECT_DESCR']['SHORT_DESCR']['P']['#text'])
                                else:
                                    if isinstance(each['OBJECT_DESCR']['SHORT_DESCR']['P'], str):
                                        obj_dsc.append(each['OBJECT_DESCR']['SHORT_DESCR']['P'])
                                    elif isinstance(each['OBJECT_DESCR']['SHORT_DESCR']['P'], list):
                                        for s in each['OBJECT_DESCR']['SHORT_DESCR']['P']:
                                            if isinstance(s, str):
                                                obj_dsc.append(s)
                                            else:
                                                obj_dsc.append(s['#text'])
                            else:
                                obj_dsc.append(''.join([item for item in each['OBJECT_DESCR']['SHORT_DESCR']['P']]))
                        if 'DURATION' in each['OBJECT_DESCR'].keys():
                            durations.append(each['OBJECT_DESCR']['DURATION']['#text']+' '+each['OBJECT_DESCR']['DURATION']['@TYPE'])
                    
                    elif isinstance(each['OBJECT_DESCR'], list):
                        for item in each['OBJECT_DESCR']:
                            if 'MAIN_SITE' in item.keys():
                                if isinstance(item['MAIN_SITE']['P'], list):
                                    for ii in item['MAIN_SITE']['P']:
                                        if isinstance(ii, dict):
                                            main_site.append(ii['#text'])
                                        elif isinstance(ii, str):
                                            main_site.append(ii)
                                        else:
                                            main_site.append(str(ii))
                                elif isinstance(item['MAIN_SITE']['P'], dict):
                                    main_site.append(item['MAIN_SITE']['P']['#text'])
                                else:
                                    main_site.append(item['MAIN_SITE']['P'])
                            if 'CPV_ADDITIONAL' in item.keys():
                                if isinstance(item['CPV_ADDITIONAL'], dict):
                                    cpv_add.append(item['CPV_ADDITIONAL']['CPV_CODE']['@CODE'])
                                else:
                                    cpv_add.append(','.join([ii['CPV_CODE']['@CODE'] for ii in item['CPV_ADDITIONAL']]))
                            if 'n2016:NUTS' in item.keys():
                                if isinstance(item['n2016:NUTS'], dict):
                                    n2016_nuts.append(item['n2016:NUTS']['@CODE'])
                                else:
                                    n2016_nuts.append(','.join([item['@CODE'] for item in item['n2016:NUTS']]))
                            if 'SHORT_DESCR' in item.keys():
                                if isinstance(item['SHORT_DESCR'], dict):
                                    if isinstance(item['SHORT_DESCR']['P'], dict):
                                        obj_dsc.append(item['SHORT_DESCR']['P']['#text'])
                                    else:
                                        if isinstance(item['SHORT_DESCR']['P'], str):
                                            obj_dsc.append(item['SHORT_DESCR']['P'])
                                        elif isinstance(item['SHORT_DESCR']['P'], list):
                                            for s in item['SHORT_DESCR']['P']:
                                                if isinstance(s, str):
                                                    obj_dsc.append(s)
                                                else:
                                                    obj_dsc.append(s['#text'])
                                else:
                                    obj_dsc.append(''.join([item for item in each['OBJECT_DESCR']['SHORT_DESCR']['P']]))
                            if 'DURATION' in item.keys():
                                durations.append(item['DURATION']['#text']+' '+item['DURATION']['@TYPE'])
                if 'DATE_PUBLICATION_NOTICE' in each.keys():
                    pub_not_date.append(each['DATE_PUBLICATION_NOTICE'])       
            
            d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site])
            d['TITLE'] = '; '.join(titles)
            d['CPV_MAIN'] = '; '.join(cpv_mains)
            d['TYPE_CONTRACT'] = '; '.join(cnt_types)
            d['SHORT_DESCR'] = ''.join(shrt_dsc)
            d['ESTIMATED_TOTAL_VALUE_CURR'] = '; '.join(val_est_tot_cur)
            d['ESTIMATED_TOTAL_VALUE'] = '; '.join(val_est_tot)
            d['NO_LOT_DIVISION'] = '; '.join([str(x) for x in no_lot_div])
            d['MAX_LOT_DIVISION'] = '; '.join([str(x) for x in max_lot])
            d['MAX_LOT_PER_TENDERER'] = '; '.join([str(x) for x in max_lot_tend])
            d['LOT_ALL'] = '; '.join([str(x) for x in lot_all])
            d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_add)
            d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(n2016_nuts)
            d['OBJECT_DESCR_SHORT_DESCR'] = '; '.join(obj_dsc)
            d['DURATION'] = '; '.join(durations)
            d['DATE_PUBLICATION_NOTICE'] = '; '.join(pub_not_date)
    
    return d

def R208_CN(doc, EN_POS):
    d = dict()
    if '@VERSION' in doc.keys():
        d['VERSION'] = doc['@VERSION']
    else:
        if isinstance(doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]], dict):
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]]['@VERSION']
        else:
            d['VERSION'] = doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]][0]['@VERSION']
    if isinstance(doc['FORM_SECTION']['CONTRACT'], dict):
        d['FORM'] = doc['FORM_SECTION']['CONTRACT']['@FORM']
        d['CONTRACTING_BODY'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['ORGANISATION']['OFFICIALNAME']
        d['CONTRACTING_BODY_ADDRESS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['ADDRESS'] if 'ADDRESS' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys() else ''
        d['CONTRACTING_BODY_TOWN'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['TOWN'] if 'TOWN' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys() else ''
        d['CONTRACTING_BODY_POSTAL_CODE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['POSTAL_CODE'] if 'POSTAL_CODE' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys() else ''
        d['CONTRACTING_BODY_COUNTRY'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['COUNTRY']['@VALUE'] if 'COUNTRY' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys() else ''
        if 'LOCATION_NUTS' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION'].keys():
            if 'n2016:NUTS' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS'].keys():
                if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS'], dict):
                    d['n2016-NUTS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS']['@CODE']
                elif isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS'], list):
                    d['n2016-NUTS'] = ','.join(item['@CODE'] for item in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS'])
            if 'LOCATION' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS'].keys():
                if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P'], list):
                    main_site = []
                    for item in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P']:
                        if isinstance(item, dict):
                            main_site.append(item['#text'])
                        elif isinstance(item, str):
                            main_site.append(item)
                        else:
                            main_site.append(str(item))
                    d['MAIN_SITE'] = ','.join(main_site)
                elif isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P'], dict):
                    d['MAIN_SITE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P']['#text']
                else:
                    d['MAIN_SITE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P']
        d['CA_TYPE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_CONTRACTING_AUTHORITY']['@VALUE'] if 'TYPE_OF_CONTRACTING_AUTHORITY' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys() else ''
        d['CA_TYPE_OTHER'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_CONTRACTING_AUTHORITY_OTHER']['@VALUE'] if 'TYPE_OF_CONTRACTING_AUTHORITY_OTHER' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys() else ''
        if 'TYPE_OF_ACTIVITY' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys():
            if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY'], list):
                d['CA_ACTIVITY'] = ','.join([item['@VALUE'] for item in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY']])
            else:
                d['CA_ACTIVITY'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY']['@VALUE']
        if 'TYPE_OF_ACTIVITY_OTHER' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys():
            if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY_OTHER'], list):
                d['CA_ACTIVITY_OTHER'] = ','.join([item['#text'] for item in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY_OTHER']])
            else:
                d['CA_ACTIVITY_OTHER'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY_OTHER']['#text']
        d['CONTRACT_TITLE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['TITLE_CONTRACT']['P']
        d['REFERENCE_NUMBER'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['FILE_REFERENCE_NUMBER']['P'] if 'FILE_REFERENCE_NUMBER' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE'].keys() else ''
        d['CPV_MAIN'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['CPV']['CPV_MAIN']['CPV_CODE']['@CODE']
        d['TYPE_CONTRACT'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['TYPE_CONTRACT_LOCATION']['TYPE_CONTRACT']['@VALUE']
        if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P'], dict):
            d['SHORT_DESCR'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P']
        else:
            shrt_dscr = []
            if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P'], list):
                for each in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P']:
                    if isinstance(each, dict):
                        shrt_dscr.append(each['#text'])
                    else:
                        shrt_dscr.append(each)
            elif isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P'], dict):
                shrt_dscr.append(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P']['#text'])
            else:
                shrt_dscr.append(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P'])
            d['SHORT_DESCR'] = ', '.join(shrt_dscr)
        if 'F02_DIVISION_INTO_LOTS' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION'].keys():
            d['NO_LOT_DIVISION'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['DIV_INTO_LOT_NO'] if 'DIV_INTO_LOT_NO' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS'].keys() else ''
            if 'F02_DIV_INTO_LOT_YES' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS'].keys():
                if isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'], list):
                    lot_no, lot_titles, obj_dsc_cpv_add, obj_dsc_n2016_nuts, obj_dsc, main_site = [],[],[],[],[],[]
                    for each in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']:
                        if 'LOT_NUMBER' in each.keys():
                            lot_no.append(each['LOT_NUMBER'])
                        if 'LOT_TITLE' in each.keys():
                            lot_titles.append(each['LOT_TITLE'])
                        if 'CPV' in each.keys():
                            obj_dsc_cpv_add.append(each['CPV']['CPV_MAIN']['CPV_CODE']['@CODE'])
                        if 'n2016:NUTS' in each.keys():
                            obj_dsc_n2016_nuts.append(each['n2016:NUTS']['@CODE'])
                        if 'LOT_DESCRIPTION' in each.keys():
                            if not isinstance(each['LOT_DESCRIPTION'], str):
                                if isinstance(each['LOT_DESCRIPTION']['P'], list):
                                    for item in each['LOT_DESCRIPTION']['P']:
                                        if isinstance(item, str):
                                            obj_dsc.append(item)
                                        elif isinstance(item, dict):
                                            obj_dsc.append(item['#text'])
                                elif isinstance(each['LOT_DESCRIPTION']['P'], dict):
                                    obj_dsc.append(each['LOT_DESCRIPTION']['P'])
                            else:
                                obj_dsc.append(each['LOT_DESCRIPTION'])              
                    d['LOT_NO'] = '; '.join(lot_no)
                    d['LOT_TITLE'] = '; '.join([str(x) if x is None else x for x in lot_titles])
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(obj_dsc_cpv_add)
                    d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(obj_dsc_n2016_nuts)
                    d['OBJECT_DESCR_SHORT_DESCR'] = '; '.join(obj_dsc)
                elif isinstance(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'], dict):
                    d['LOT_NO'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['LOT_NUMBER'] if 'LOT_NUMBER' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys() else ''
                    d['LOT_TITLE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['LOT_TITLE'] if 'LOT_TITLE' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys() else ''
                    d['OBJECT_DESCR_CPV_ADDITIONAL'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['CPV']['CPV_MAIN']['CPV_CODE']['@CODE'] if 'CPV' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys() else ''
                    d['OBJECT_DESCR_n2016-NUTS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['n2016:NUTS']['@CODE'] if 'n2016:NUTS' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys() else ''
                    d['OBJECT_DESCR_SHORT_DESCR'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['LOT_DESCRIPTION']['P'] if 'LOT_DESCRIPTION' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys() else ''

        if 'PERIOD_WORK_DATE_STARTING' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION'].keys():
            if 'INTERVAL_DATE' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING'].keys():
                d['OBJECT_DESCR_DURATION'] = dt.strptime(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['END_DATE']['DAY']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['END_DATE']['MONTH']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['END_DATE']['YEAR'], '%d-%m-%Y') - dt.strptime(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['START_DATE']['DAY']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['START_DATE']['MONTH']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['START_DATE']['YEAR'], '%d-%m-%Y')
            else:
                d['OBJECT_DESCR_DURATION'] = list(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING'].values())[0] + ' ' + list(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING'].keys())[0]
        d['PROCEDURE_DATE_RECEIPT_TENDERS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['DAY']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['MONTH']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['YEAR']
        d['PROCEDURE_TIME_RECEIPT_TENDERS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['TIME'] if 'TIME' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE'].keys() else ''
        if 'MINIMUM_TIME_MAINTAINING_TENDER' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE'].keys():
            if 'UNTIL_DATE' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER'].keys():
                d['TENDER_MAINTAINING_UNTIL'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER']['UNTIL_DATE']['DAY']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER']['UNTIL_DATE']['MONTH']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER']['UNTIL_DATE']['YEAR']
            else:
                d['TENDER_MAINTAINING_MIN_TIME'] = list(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER'].values())[0]+' '+list(doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER'].keys())[0]
        if 'CONDITIONS_FOR_OPENING_TENDERS' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE'].keys():
            if 'DATE_TIME' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS'].keys():
                d['PROCEDURE_DATE_OPENING_TENDERS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['DAY']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['MONTH']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['YEAR']
                if 'TIME' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME'].keys():
                    d['PROCEDURE_TIME_OPENING_TENDERS'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['TIME'] if 'DATE_TIME' in doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS'].keys() else ''
        d['DATE_DISPATCH_NOTICE'] = doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE']['NOTICE_DISPATCH_DATE']['DAY']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE']['NOTICE_DISPATCH_DATE']['MONTH']+'-'+doc['FORM_SECTION']['CONTRACT']['FD_CONTRACT']['COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE']['NOTICE_DISPATCH_DATE']['YEAR']

    elif isinstance(doc['FORM_SECTION']['CONTRACT'], list):
        form, ca_body, ca_add, ca_town, ca_postal, ca_country, n2016_nuts, ca_type, ca_type_oth, ca_act, ca_act_oth, cont_title = [],[],[],[],[],[],[],[],[],[],[],[]
        ref_num, cpv_main, cont_type, shrt_dsc, no_lot_div, lot_num, lot_title, cpv_add, obj_dsc_n2016_nuts, obj_dsc_shrt_dsc = [],[],[],[],[],[],[],[],[],[]
        obj_dsc_duration, proc_receipt_date, proc_receipt_time, tender_maint, proc_open_date, proc_open_time, dispatch_date, main_site = [],[],[],[],[],[],[],[]
        for each in doc['FORM_SECTION']['CONTRACT']:
            form.append(each['@FORM'])
            ca_body.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['ORGANISATION']['OFFICIALNAME'])
            if 'ADDRESS' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys():
                ca_add.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['ADDRESS'])
            if 'TOWN' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys():
                ca_town.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['TOWN'])
            if 'POSTAL_CODE' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys():
                ca_postal.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['POSTAL_CODE'])
            if 'COUNTRY' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE'].keys():
                ca_country.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['NAME_ADDRESSES_CONTACT_CONTRACT']['CA_CE_CONCESSIONAIRE_PROFILE']['COUNTRY']['@VALUE'])
            if 'LOCATION_NUTS' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION'].keys():
                if 'n2016:NUTS' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS'].keys():
                    if isinstance(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS'], dict):
                        n2016_nuts.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS']['@CODE'])
                    elif isinstance(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS'], list):
                        n2016_nuts.append(','.join(item['@CODE'] for item in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['n2016:NUTS']))
                if 'LOCATION' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS'].keys():
                    if isinstance(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P'], list):
                        main_site.append(','.join(item if isinstance(item,str) else str(item['#text']) for item in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P']))
                    elif isinstance(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P'], dict):
                        main_site.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P']['#text'])
                    else:
                        main_site.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['LOCATION_NUTS']['LOCATION']['P'])
            if 'TYPE_OF_CONTRACTING_AUTHORITY' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys():
                ca_type.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_CONTRACTING_AUTHORITY']['@VALUE'])
            if 'TYPE_OF_CONTRACTING_AUTHORITY_OTHER' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys():
                ca_type_oth.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_CONTRACTING_AUTHORITY_OTHER']['@VALUE'])
            if 'TYPE_OF_ACTIVITY' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys():
                ca_act.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY']['@VALUE'])
            if 'TYPE_OF_ACTIVITY_OTHER' in each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES'].keys():
                ca_act_oth.append(each['FD_CONTRACT']['CONTRACTING_AUTHORITY_INFORMATION']['TYPE_AND_ACTIVITIES_AND_PURCHASING_ON_BEHALF']['TYPE_AND_ACTIVITIES']['TYPE_OF_ACTIVITY_OTHER']['#text'])
            cont_title.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['TITLE_CONTRACT']['P'])
            if 'FILE_REFERENCE_NUMBER' in each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE'].keys():
                ref_num.append(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['FILE_REFERENCE_NUMBER']['P'])
            cpv_main.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['CPV']['CPV_MAIN']['CPV_CODE']['@CODE'])
            cont_type.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['TYPE_CONTRACT_LOCATION']['TYPE_CONTRACT']['@VALUE'])
            shrt_dsc.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['SHORT_CONTRACT_DESCRIPTION']['P'])
            if 'F02_DIVISION_INTO_LOTS' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION'].keys():
                if 'DIV_INTO_LOT_NO' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS'].keys():
                    no_lot_div.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['DIV_INTO_LOT_NO'])
                if 'F02_DIV_INTO_LOT_YES' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS'].keys():
                    if isinstance(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'], list):
                        f02_lot_no, f02_lot_titles, f02_obj_dsc_cpv_add, f02_obj_dsc_n2016_nuts, f02_obj_dsc = [],[],[],[],[]
                        for item in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']:
                            if 'LOT_NUMBER' in item.keys():
                                f02_lot_no.append(item['LOT_NUMBER'])
                            if 'LOT_TITLE' in item.keys():
                                f02_lot_titles.append(item['LOT_TITLE'])
                            if 'CPV' in item.keys():
                                f02_obj_dsc_cpv_add.append(item['CPV']['CPV_MAIN']['CPV_CODE']['@CODE'])
                            if 'n2016:NUTS' in item.keys():
                                f02_obj_dsc_n2016_nuts.append(item['n2016:NUTS']['@CODE'])
                            if 'LOT_DESCRIPTION' in item.keys():
                                if not isinstance(item['LOT_DESCRIPTION'], str):
                                    if isinstance(item['LOT_DESCRIPTION']['P'], list):
                                        f02_obj_dsc.append(','.join(item['LOT_DESCRIPTION']['P']))
                                    else:
                                        f02_obj_dsc.append(item['LOT_DESCRIPTION']['P'])
                                else:
                                    f02_obj_dsc.append(item['LOT_DESCRIPTION'])
                        lot_num.append(','.join(f02_lot_no))
                        lot_title.append(','.join([str(x) if x is None else x for x in f02_lot_titles]))
                        cpv_add.append(','.join(f02_obj_dsc_cpv_add))
                        obj_dsc_n2016_nuts.append(','.join(f02_obj_dsc_n2016_nuts))
                        obj_dsc_shrt_dsc(','.join(f02_obj_dsc))
                    elif isinstance(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'], dict):
                        if 'LOT_NUMBER' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys():
                            lot_num.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['LOT_NUMBER'])
                        if 'LOT_TITLE' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys():
                            lot_title.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['LOT_TITLE'])
                        if 'CPV' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys():
                            cpv_add.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['CPV']['CPV_MAIN']['CPV_CODE']['@CODE'])
                        if 'n2016:NUTS' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys():
                            obj_dsc_n2016_nuts.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['n2016:NUTS']['@CODE'])
                        if 'LOT_DESCRIPTION' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B'].keys():
                            obj_dsc_shrt_dsc.append(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['DESCRIPTION_CONTRACT_INFORMATION']['F02_DIVISION_INTO_LOTS']['F02_DIV_INTO_LOT_YES']['F02_ANNEX_B']['LOT_DESCRIPTION']['P'])
            if 'PERIOD_WORK_DATE_STARTING' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION'].keys():
                if 'INTERVAL_DATE' in each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING'].keys():
                    obj_dsc_duration.append(dt.strptime(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['END_DATE']['DAY']+'-'+each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['END_DATE']['MONTH']+'-'+each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['END_DATE']['YEAR'], '%d-%m-%Y') - dt.strptime(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['START_DATE']['DAY']+'-'+each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['START_DATE']['MONTH']+'-'+each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING']['INTERVAL_DATE']['START_DATE']['YEAR'], '%d-%m-%Y'))
                else:
                    obj_dsc_duration.append(list(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING'].values())[0] + ' ' + list(each['FD_CONTRACT']['OBJECT_CONTRACT_INFORMATION']['PERIOD_WORK_DATE_STARTING'].keys())[0])
            proc_receipt_date.append(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['DAY']+'-'+each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['MONTH']+'-'+each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['YEAR'])
            if 'TIME' in each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE'].keys():
                proc_receipt_time.append(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['RECEIPT_LIMIT_DATE']['TIME'])
            if 'MINIMUM_TIME_MAINTAINING_TENDER' in each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE'].keys():
                tender_maint.append(list(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER'].values())[0]+' '+list(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['MINIMUM_TIME_MAINTAINING_TENDER'].keys())[0])
            if 'CONDITIONS_FOR_OPENING_TENDERS' in each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE'].keys():
                if 'DATE_TIME' in each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS'].keys():
                    proc_open_date.append(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['DAY']+'-'+each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['MONTH']+'-'+each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['YEAR'])
                    if 'TIME' in each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME'].keys():
                        proc_open_time.append(each['FD_CONTRACT']['PROCEDURE_DEFINITION_CONTRACT_NOTICE']['ADMINISTRATIVE_INFORMATION_CONTRACT_NOTICE']['CONDITIONS_FOR_OPENING_TENDERS']['DATE_TIME']['TIME'])
            dispatch_date.append(each['FD_CONTRACT']['COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE']['NOTICE_DISPATCH_DATE']['DAY']+'-'+each['FD_CONTRACT']['COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE']['NOTICE_DISPATCH_DATE']['MONTH']+'-'+each['FD_CONTRACT']['COMPLEMENTARY_INFORMATION_CONTRACT_NOTICE']['NOTICE_DISPATCH_DATE']['YEAR'])
        d['FORM'] = '; '.join(form)
        d['CONTRACTING_BODY'] = '; '.join(ca_body)
        d['CONTRACTING_BODY_ADDRESS'] = '; '.join(ca_add)
        d['CONTRACTING_BODY_TOWN'] = '; '.join(ca_town)
        d['CONTRACTING_BODY_POSTAL_CODE'] = '; '.join(ca_postal)
        d['CONTRACTING_BODY_COUNTRY'] = '; '.join(ca_country)
        d['n2016-NUTS'] = '; '.join(n2016_nuts)
        d['MAIN_SITE'] = '; '.join([str(x) if x is None else x for x in main_site])
        d['CA_TYPE'] = '; '.join(ca_type)
        d['CA_TYPE_OTHER'] = '; '.join(ca_type_oth)
        d['CA_ACTIVITY'] = '; '.join(ca_act)
        d['CA_ACTIVITY_OTHER'] = '; '.join(ca_act_oth)
        d['CONTRACT_TITLE'] = '; '.join(cont_title)
        d['REFERENCE_NUMBER'] = '; '.join(ref_num)
        d['CPV_MAIN'] = '; '.join(cpv_main)
        d['TYPE_CONTRACT'] = '; '.join(cont_type)
        d['SHORT_DESCR'] = '; '.join(shrt_dsc)
        d['NO_LOT_DIVISION'] = '; '.join([str(x) for x in no_lot_div])
        d['LOT_NUMBER'] = '; '.join(lot_num)
        d['LOT_TITLE'] = '; '.join(lot_title)
        d['OBJECT_DESCR_CPV_ADDITIONAL'] = '; '.join(cpv_add)
        d['OBJECT_DESCR_n2016-NUTS'] = '; '.join(obj_dsc_n2016_nuts)
        d['OBJECT_DESCR_SHORT_DESCR'] = '; '.join(obj_dsc_shrt_dsc)
        d['OBJECT_DESCR_DURATION'] = '; '.join(obj_dsc_duration)
        d['PROCEDURE_DATE_RECEIPT_TENDERS'] = '; '.join(proc_receipt_date)
        d['PROCEDURE_TIME_RECEIPT_TENDERS'] = '; '.join(proc_receipt_time)
        d['TENDER_MAINTAINING_MIN_TIME'] = '; '.join(tender_maint)
        d['PROCEDURE_DATE_OPENING_TENDERS'] = '; '.join(proc_open_date)
        d['PROCEDURE_TIME_OPENING_TENDERS'] = '; '.join(proc_open_time)
        d['DATE_DISPATCH_NOTICE'] = '; '.join(dispatch_date)
    
    return d

def get_version(doc):    
    # VERSION
    if '@VERSION' in doc.keys():
        return doc['@VERSION']
    else:
        if isinstance(doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]], dict):
            return doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]]['@VERSION']
        else:
            return doc['FORM_SECTION'][list(doc['FORM_SECTION'].keys())[0]][0]['@VERSION']
            
def get_lang_pos(doc):
    # EN_POS
    if 'EN' in doc['TECHNICAL_SECTION']['FORM_LG_LIST']:
        return doc['TECHNICAL_SECTION']['FORM_LG_LIST'].split(' ').index('EN')
    else:
        return 0

def get_form_data(doc):
    version = get_version(doc)
    EN_POS = get_lang_pos(doc)
    
    if version[:6] == 'R2.0.9':
        #Prior Information Notice
        if 'F01_2014' in doc['FORM_SECTION'].keys():
            return R209_PIN(doc, EN_POS)
        #Contract Notice
        elif 'F02_2014' in doc['FORM_SECTION'].keys():
            return R209_CN(doc, EN_POS)
        #Contract Award Notice
        elif 'F03_2014' in doc['FORM_SECTION'].keys():
            return R209_CAN(doc, EN_POS)
        else:
            return 0
    elif version[:6] == 'R2.0.8':
        #Prior Information Notice
        if 'PRIOR_INFORMATION' in doc['FORM_SECTION'].keys():
            return 0
        #Contract Notice
        elif 'CONTRACT' in doc['FORM_SECTION'].keys():
            return 0 #R208_CN(doc, EN_POS)
        #Contract Award Notice
        elif 'CONTRACT_AWARD' in doc['FORM_SECTION'].keys():
            return 0
        else:
            return 0


def get_contract_nature(doc):
    return doc['CODED_DATA_SECTION']['CODIF_DATA']['NC_CONTRACT_NATURE']['#text']

# to call all parsing helper functions & parse the XML
def parse(dirName):
    xmlFilesList = list()
    for (dirpath, dirnames, filenames) in os.walk(dirName):
        xmlFilesList += [os.path.join(dirpath, file) for file in filenames]
    
    for i, file in enumerate(xmlFilesList):
        if file.endswith('.xml'):
            print('[info] {} ... '.format(file.replace('\\','/')), end='')
            with open(file, encoding='utf8') as f:
                doc = xmltodict.parse(f.read())['TED_EXPORT']

            CONTRACT_NATURE = get_contract_nature(doc)
            if CONTRACT_NATURE == 'Services':
                FORM_DATA = get_form_data(doc)
                if FORM_DATA != 0:
                    EN_POS = get_lang_pos(doc)
                    VERSION = get_version(doc)
                    CODED_DATA = coded_and_translation_data(doc, EN_POS)
                    row = {'LANG': doc['TECHNICAL_SECTION']['FORM_LG_LIST'].split(' ')[EN_POS], 'VERSION': VERSION, **CODED_DATA, **FORM_DATA}
                    pd.DataFrame(row, index=[i]).to_csv(file.replace('\\', '/').split('.')[0] + '.csv')
                    print('parsed!')
                    del row
                else:
                    print('')
            else:
                print('')
        
# only EDIT this: path for all uncompressed XMLs
dirName = 'FTP_Data/2020/'

# Runner
print('[info] Parsing XMLs ...')
print(dt.now())
parse(dirName)
print(dt.now())

# Merger
print('[info] Merging the CSVs ...')
print(dt.now())
for subdir in os.listdir(dirName):
    out_filename = os.path.join(dirName, subdir)
    print("[info] merging all csv files in '{}'".format(out_filename))
    combined_csv = pd.DataFrame()
    for subsubdir in os.listdir(os.path.join(dirName, subdir)):
        path = os.path.join(dirName, subdir, subsubdir).replace('\\','/')
        for root, subsubdir, files in os.walk(path):
            for file in tqdm(files):
                if file[-4:] == '.csv':
                    combined_csv = pd.concat([combined_csv, pd.read_csv(os.path.join(path, file))], sort=True)
    combined_csv.to_csv(out_filename+'.csv', index=False, encoding='utf-8')
    print("[info] saved '{}' to disk".format(out_filename+'.csv'))
    del combined_csv
print(dt.now())