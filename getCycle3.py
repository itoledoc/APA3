__author__ = 'itoledo'

import os
import pandas as pd
import cx_Oracle
from XmlProjParsers3 import *

prj = '{Alma/ObsPrep/ObsProject}'
val = '{Alma/ValueTypes}'
sbl = '{Alma/ObsPrep/SchedBlock}'

pd.options.display.width = 200
pd.options.display.max_columns = 100

# get Cycle3 APDM

conx_string = os.environ['CON_STR']
connection = cx_Oracle.connect(conx_string)
cursor = connection.cursor()

sql1 = str(
    "SELECT PRJ_ARCHIVE_UID as OBSPROJECT_UID,PI,PRJ_NAME,CODE,"
    "PRJ_SCIENTIFIC_RANK,PRJ_VERSION,PRJ_LETTER_GRADE,"
    "DOMAIN_ENTITY_STATE as PRJ_STATUS, ARCHIVE_UID as OBSPROPOSAL_UID "
    "FROM ALMA.BMMV_OBSPROJECT obs1, ALMA.OBS_PROJECT_STATUS obs2, "
    "ALMA.BMMV_OBSPROPOSAL obs3 "
    "WHERE regexp_like (CODE, '^2015\..*\.[AST]') AND "
    "obs2.OBS_PROJECT_ID = obs1.PRJ_ARCHIVE_UID AND "
    "obs1.PRJ_ARCHIVE_UID = obs3.PROJECTUID")


cursor.execute(sql1)


df1 = pd.DataFrame(
    cursor.fetchall(), columns=[rec[0] for rec in cursor.description])


obsproject_uids = df1.OBSPROJECT_UID.unique()

for uid in obsproject_uids:
    cursor.execute(
        "SELECT TIMESTAMP, XMLTYPE.getClobVal(xml) "
        "FROM ALMA.XML_OBSPROJECT_ENTITIES "
        "WHERE ARCHIVE_UID = '%s'" % uid)
    try:
        data = cursor.fetchall()[0]
        xml_content = data[1].read()
        xmlfilename = uid.replace('://', '___').replace('/', '_') + '.xml'
        filename = 'cycle3/obsproject/' + xmlfilename
        io_file = open(filename, 'w')
        io_file.write(xml_content)
        io_file.close()
    except IndexError:
        print("Project %s not found on archive?" %
               uid)

for r in os.listdir('cycle3/obsproject'):
    if r.startswith('uid'):
        obsparse = ObsProject('cycle3/obsproject/' + r)
        obspropuid = obsparse.ObsProposalRef.attrib['entityId']
        try:
            obsrevuid = obsparse.ObsReviewRef.attrib['entityId']
        except AttributeError:
            print("Obsproject %s has no ObsReviewRef" % r)
            continue

        cursor.execute(
            "SELECT TIMESTAMP, XMLTYPE.getClobVal(xml) "
            "FROM ALMA.XML_OBSPROPOSAL_ENTITIES "
            "WHERE ARCHIVE_UID = '%s'" % obspropuid)

        try:
            data = cursor.fetchall()[0]
            xml_content = data[1].read()
            xmlfilename = obspropuid.replace('://', '___').replace('/', '_') + \
                          '.xml'
            filename = 'cycle3/obsproposal/' + xmlfilename
            io_file = open(filename, 'w')
            io_file.write(xml_content)
            io_file.close()
        except IndexError:
            print("Proposal %s not found on archive?" %
                   obspropuid)
            continue

        cursor.execute(
            "SELECT TIMESTAMP, XMLTYPE.getClobVal(xml) "
            "FROM ALMA.XML_OBSREVIEW_ENTITIES "
            "WHERE ARCHIVE_UID = '%s'" % obsrevuid)

        try:
            data = cursor.fetchall()[0]
            xml_content = data[1].read()
            xmlfilename = obsrevuid.replace('://', '___').replace('/', '_') + \
                          '.xml'
            filename = 'cycle3/obsreview/' + xmlfilename
            io_file = open(filename, 'w')
            io_file.write(xml_content)
            io_file.close()
        except IndexError:
            print("Review %s not found on archive?" %
                   obsrevuid)
            continue


for r in os.listdir('cycle3/obsreview/'):
    if not r.startswith('uid'):
        continue
    obsreview = ObsReview('cycle3/obsreview/' + r)
    op = obsreview.data.findall('.//' + prj + 'SchedBlockRef')
    for sbref in op:
        sbuid = sbref.attrib['entityId']
        cursor.execute(
            "SELECT TIMESTAMP, XMLTYPE.getClobVal(xml) "
            "FROM ALMA.XML_SCHEDBLOCK_ENTITIES "
            "WHERE ARCHIVE_UID = '%s'" % sbuid)
        try:
            data = cursor.fetchall()[0]
            xml_content = data[1].read()
            xmlfilename = sbuid.replace('://', '___').replace('/', '_') + '.xml'
            filename = 'cycle3/schedblock/' + xmlfilename
            io_file = open(filename, 'w')
            io_file.write(xml_content)
            io_file.close()
        except IndexError:
            print("SB %s not found on archive?" %
                   sbuid)
            continue