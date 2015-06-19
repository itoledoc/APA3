__author__ = 'itoledo'


import os
import sys
import pandas as pd
import ephem
import cx_Oracle
import arrayResolution2p3 as ARes

from collections import namedtuple
from subprocess import call
from XmlProjParsers3 import *
from converter3 import *

prj = '{Alma/ObsPrep/ObsProject}'
val = '{Alma/ValueTypes}'
sbl = '{Alma/ObsPrep/SchedBlock}'

pd.options.display.width = 200
pd.options.display.max_columns = 100

confDf = pd.DataFrame(
    [('C36-1', 3.4, 2.3, 1.5, 1.0, 0.7, 0.5, 0.4)],
    columns=['Conf', 'ALMA_RB_03', 'ALMA_RB_04', 'ALMA_RB_06', 'ALMA_RB_07',
             'ALMA_RB_08', 'ALMA_RB_09', 'ALMA_RB_10'],
    index=['C36-1'])
confDf.ix['C36-2'] = ('C36-2', 1.8, 1.2, 0.8, 0.5, 0.4, 0.3, 0.2)
confDf.ix['C36-3'] = ('C36-3', 1.2, 0.8, 0.5, 0.4, 0.3, 0.2, 0.1)
confDf.ix['C36-4'] = ('C36-4', 0.7, 0.5, 0.3, 0.2, 0.15, 0.1, 0.08)
confDf.ix['C36-5'] = ('C36-5', 0.5, 0.3, 0.2, 0.14, 0.1, 0.07, 0.06)
confDf.ix['C36-6'] = ('C36-6', 0.3, 0.2, 0.1, 0.08, 0.06, 0.04, 0.03)
confDf.ix['C36-7'] = ('C36-7', 0.1, 0.08, 0.05, 0.34, None, None, None)
confDf.ix['C36-8'] = ('C36-8', 0.075, 0.05, 0.03, None, None, None, None)
Range = namedtuple('Range', ['start', 'end'])

conflim = pd.Series({'C36-1': 2.8849999999999998,
                     'C36-2': 1.72,
                     'C36-3': 1.2549999999999999,
                     'C36-4': 0.93000000000000005,
                     'C36-5': 0.65999999999999992,
                     'C36-6': 0.48999999999999999,
                     'C36-7': 0.20499999999999999,
                     'C36-8': 0.0})


class Database(object):

    """
    Database is the class that stores the Projects and SB information in
    dataframes, and it also has the methods to connect and query the OSF
    archive for this info.

    A default instance will use the directory $HOME/.wto as a cache, and by
    default find the approved Cycle 2 projects and carried-over Cycle 1
    projects. If a file name or list are given as 'source' parameter, only the
    information of the projects in that list or filename will be ingested.

    Setting *forcenew* to True will force the cleaning of the cache dir, and
    all information will be processed again.

    :param path: Path for data cache.
    :type path: str, default '$HOME/.wto'
    :param forcenew: Force cache cleaning and reload from archive.
    :type forcenew: boolean, default False
    """

    def __init__(self, path='/.apa3/', forcenew=False, verbose=True):
        """


        """

        self.new = forcenew
        # Default Paths and Preferences
        if path[-1] != '/':
            path += '/'
        self.path = os.environ['HOME'] + path
        self.apa_path = os.environ['APA3']
        self.phase1_data = os.environ['PHASEONE_C3']
        self.sbxml = self.path + 'schedblock/'
        self.obsxml = self.path + 'obsproject/'
        self.propxml = self.path + 'obsproposal/'
        self.reviewxml = self.path + 'obsreview/'
        self.preferences = pd.Series(
            ['project.pandas', 'sciencegoals.pandas',
             'scheduling.pandas', 'special.list', 'pwvdata.pandas',
             'executive.pandas', 'sbxml_table.pandas', 'sbinfo.pandas',
             'newar.pandas', 'fieldsource.pandas', 'target.pandas',
             'spectralconf.pandas'],
            index=['project_table', 'sciencegoals_table',
                   'scheduling_table', 'special', 'pwv_data',
                   'executive_table', 'sbxml_table', 'sbinfo_table',
                   'newar_table', 'fieldsource_table', 'target_table',
                   'spectralconf_table'])
        self.status = ["Canceled", "Rejected"]
        self.verbose = verbose

        # self.grades = pd.read_table(
        #     self.apa_path + 'conf/c2grade.csv', sep=',')
        # self.sb_sg_p1 = pd.read_pickle(self.apa_path + 'conf/sb_sg_p1.pandas')

        # Global SQL search expressions
        # Search Project's PT information and match with PT Status
        self.sql1 = str(
            "SELECT PRJ_ARCHIVE_UID as OBSPROJECT_UID,PI,PRJ_NAME,"
            "CODE,PRJ_SCIENTIFIC_RANK,PRJ_VERSION,"
            "PRJ_LETTER_GRADE,DOMAIN_ENTITY_STATE as PRJ_STATUS,"
            "ARCHIVE_UID as OBSPROPOSAL_UID "
            "FROM ALMA.BMMV_OBSPROJECT obs1, ALMA.OBS_PROJECT_STATUS obs2,"
            " ALMA.BMMV_OBSPROPOSAL obs3 "
            "WHERE regexp_like (CODE, '^2015\..*\.[AST]') "
            "AND (PRJ_LETTER_GRADE='A' OR PRJ_LETTER_GRADE='B' "
            "OR PRJ_LETTER_GRADE='C') AND PRJ_SCIENTIFIC_RANK < 9999 "
            "AND obs2.OBS_PROJECT_ID = obs1.PRJ_ARCHIVE_UID AND "
            "obs1.PRJ_ARCHIVE_UID = obs3.PROJECTUID")

        conx_string = os.environ['CON_STR']
        self.connection = cx_Oracle.connect(conx_string)
        self.cursor = self.connection.cursor()

        # Initialize with saved data and update, Default behavior.
        if not self.new:
            try:
                self.projects = pd.read_pickle(
                    self.path + 'projects.pandas')
                self.sb_sg_p2 = pd.read_pickle(
                    self.path + 'sb_sg_p2.pandas')
                self.sciencegoals = pd.read_pickle(
                    self.path + 'sciencegoals.pandas')
                self.aqua_execblock = pd.read_pickle(
                    self.path + 'aqua_execblock.pandas')
                self.executive = pd.read_pickle(
                    self.path + 'executive.pandas')
                self.obsprojects = pd.read_pickle(
                    self.path + 'obsprojects.pandas')
                self.obsproposals = pd.read_pickle(
                    self.path + 'obsproposals.pandas')
                self.saos_obsproject = pd.read_pickle(
                    self.path + 'saos_obsproject.pands')
                self.saos_schedblock = pd.read_pickle(
                    self.path + 'saos_schedblock.pandas')
                self.sg_targets = pd.read_pickle(
                    self.path + 'sg_targets')
            except IOError, e:
                print e
                self.new = True

        # Create main dataframes
        if self.new:
            call(['rm', '-rf', self.path])
            print(self.path + ": creating preferences dir")
            os.mkdir(self.path)
            os.mkdir(self.sbxml)
            os.mkdir(self.obsxml)
            os.mkdir(self.propxml)
            # Global Oracle Connection

            # Populate different dataframes related to projects and SBs statuses
            # self.scheduling_proj: data frame with projects at SCHEDULING_AOS
            # Query Projects currently on SCHEDULING_AOS
            self.sqlsched_proj = str(
                "SELECT CODE,OBSPROJECT_UID as OBSPROJECT_UID,"
                "VERSION as PRJ_SAOS_VERSION, STATUS as PRJ_SAOS_STATUS "
                "FROM SCHEDULING_AOS.OBSPROJECT "
                "WHERE regexp_like (CODE, '^2015\..*\.[AST]')")
            self.cursor.execute(self.sqlsched_proj)
            self.saos_obsproject = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('CODE', drop=False)

            # self.scheduling_sb: SBs at SCHEDULING_AOS
            # Query SBs in the SCHEDULING_AOS tables
            self.sqlsched_sb = str(
                "SELECT ou.OBSUNIT_UID as OUS_ID, sb.NAME as SB_NAME,"
                "sb.SCHEDBLOCK_CTRL_EXEC_COUNT,"
                "sb.SCHEDBLOCK_CTRL_STATE as SB_SAOS_STATUS,"
                "ou.OBSUNIT_PROJECT_UID as OBSPROJECT_UID "
                "FROM SCHEDULING_AOS.SCHEDBLOCK sb, SCHEDULING_AOS.OBSUNIT ou "
                "WHERE sb.SCHEDBLOCKID = ou.OBSUNITID AND sb.CSV = 0")
            self.cursor.execute(self.sqlsched_sb)
            self.saos_schedblock = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('OUS_ID', drop=False)

            # self.sbstates: SBs status (PT?)
            # Query SBs status
            self.sqlstates = str(
                "SELECT DOMAIN_ENTITY_STATE as SB_STATE,"
                "DOMAIN_ENTITY_ID as SB_UID,OBS_PROJECT_ID as OBSPROJECT_UID "
                "FROM ALMA.SCHED_BLOCK_STATUS")
            self.cursor.execute(self.sqlstates)
            self.sb_status = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('SB_UID', drop=False)

            # self.qa0: QAO flags for observed SBs
            # Query QA0 flags from AQUA tables
            self.sqlqa0 = str(
                "SELECT SCHEDBLOCKUID as SB_UID,QA0STATUS,STARTTIME,ENDTIME,"
                "EXECBLOCKUID,EXECFRACTION "
                "FROM ALMA.AQUA_EXECBLOCK "
                "WHERE regexp_like (OBSPROJECTCODE, '^2015\..*\.[AST]')")

            self.cursor.execute(self.sqlqa0)
            self.aqua_execblock = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('SB_UID', drop=False)

            # Query for Executives
            sql2 = str(
                "SELECT PROJECTUID as OBSPROJECT_UID, ASSOCIATEDEXEC "
                "FROM ALMA.BMMV_OBSPROPOSAL "
                "WHERE regexp_like (CYCLE, '^2015.[1A]')")
            self.cursor.execute(sql2)
            self.executive = pd.DataFrame(
                self.cursor.fetchall(), columns=['OBSPROJECT_UID', 'EXEC'])

            self.start_wto()

        self.sqlstates = str(
            "SELECT DOMAIN_ENTITY_STATE as SB_STATE,"
            "DOMAIN_ENTITY_ID as SB_UID,OBS_PROJECT_ID as OBSPROJECT_UID "
            "FROM ALMA.SCHED_BLOCK_STATUS")
        self.cursor.execute(self.sqlstates)
        self.sb_status = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        ).set_index('SB_UID', drop=False)

    def start_wto(self):

        """
        Initializes the wtoDatabase dataframes.

        The function queries the archive to look for cycle 1 and cycle 2
        projects, disregarding any projects with status "Approved",
        "Phase1Submitted", "Broken", "Canceled" or "Rejected".

        The archive tables used are ALMA.BMMV_OBSPROPOSAL,
        ALMA.OBS_PROJECT_STATUS, ALMA.BMMV_OBSPROJECT and
        ALMA.XML_OBSPROJECT_ENTITIES.

        :return: None
        """
        # noinspection PyUnusedLocal
        status = self.status

        # Query for Projects, from BMMV.
        self.cursor.execute(self.sql1)
        df1 = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description])
        print(len(df1.query('PRJ_STATUS not in @status')))
        self.projects = pd.merge(
            df1.query('PRJ_STATUS not in @status'), self.executive,
            on='OBSPROJECT_UID'
        ).set_index('CODE', drop=False)

        timestamp = pd.Series(
            np.zeros(len(self.projects), dtype=object),
            index=self.projects.index)
        self.projects['timestamp'] = timestamp
        self.projects['xmlfile'] = pd.Series(
            np.zeros(len(self.projects), dtype=object),
            index=self.projects.index)

        # # Download and read obsprojects and obsprosal
        # number = self.projects.__len__()
        # c = 1
        # for r in self.projects.iterrows():
        #     xmlfilename, obsproj = self.get_projectxml(
        #         r[1].CODE, r[1].PRJ_STATUS, number, c, verbose=self.verbose)
        #     c += 1
        #     if obsproj:
        #         self.read_obsproject(xmlfilename)
        #     else:
        #         print(r[1].CODE + " (read obsproposal)")
        #         self.read_obsproposal(xmlfilename, r[1].CODE)
        #
        # self.projects['isCycle2'] = self.projects.apply(
        #     lambda r1: True if r1['CODE'].startswith('2013') else False,
        #     axis=1)
        # self.projects.to_pickle(
        #     self.path + 'projects.pandas')
        # self.sb_sg_p2.to_pickle(
        #     self.path + 'sb_sg_p2.pandas')
        # self.sciencegoals.to_pickle(
        #     self.path + 'sciencegoals.pandas')
        # self.aqua_execblock.to_pickle(
        #     self.path + 'aqua_execblock.pandas')
        # self.executive.to_pickle(
        #     self.path + 'executive.pandas')
        # self.obsprojects.to_pickle(
        #     self.path + 'obsprojects.pandas')
        # self.obsproposals.to_pickle(
        #     self.path + 'obsproposals.pandas')
        # self.saos_obsproject.to_pickle(
        #     self.path + 'saos_obsproject.pands')
        # self.saos_schedblock.to_pickle(
        #     self.path + 'saos_schedblock.pandas')
        # self.sg_targets.to_pickle(
        #     self.path + 'sg_targets')