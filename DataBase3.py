import os
import sys
# import pandas as pd
# import ephem
import arrayResolutionCy3 as ARes
import cx_Oracle

from collections import namedtuple
from subprocess import call
from XmlParsers3 import *
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

conflim = pd.Series(
    {'C36-1': 2.8849999999999998,
     'C36-2': 1.72,
     'C36-3': 1.2549999999999999,
     'C36-4': 0.93000000000000005,
     'C36-5': 0.65999999999999992,
     'C36-6': 0.48999999999999999,
     'C36-7': 0.20499999999999999,
     'C36-8': 0.0})

phase_i_status = ["Phase1Submitted", "Rejected", "Approved"]


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
        Initialize the APA ETL database

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

        self.obsproject = pd.DataFrame()
        self.ares = ARes.arrayRes()

        # self.grades = pd.read_table(
        #     self.apa_path + 'conf/DC_final modified gradeC.csv', sep=',')
        # self.sb_sg_p1 = pd.read_pickle(self.apa_path + 'conf/sb_sg_p1.pandas')

        # Global SQL search expressions
        # Search Project's PT information and match with PT Status
        self.sql1 = str(
            "SELECT obs1.PRJ_ARCHIVE_UID as OBSPROJECT_UID, obs1.PI, "
            "obs1.PRJ_NAME,"
            "CODE,PRJ_SCIENTIFIC_RANK,PRJ_VERSION,"
            "PRJ_LETTER_GRADE,DOMAIN_ENTITY_STATE as PRJ_STATUS,"
            "obs3.ARCHIVE_UID as OBSPROPOSAL_UID, obs4.DC_LETTER_GRADE,"
            "obs3.CYCLE "
            "FROM ALMA.BMMV_OBSPROJECT obs1, ALMA.OBS_PROJECT_STATUS obs2,"
            " ALMA.BMMV_OBSPROPOSAL obs3, ALMA.PROPOSAL obs4 "
            "WHERE regexp_like (CODE, '^201[35]\..*\.[AST]') "
            "AND obs2.OBS_PROJECT_ID = obs1.PRJ_ARCHIVE_UID AND "
            "obs1.PRJ_ARCHIVE_UID = obs3.PROJECTUID AND "
            "obs4.ARCHIVE_UID = obs3.ARCHIVE_UID AND "
            "obs4.DC_LETTER_GRADE IN ('A', 'B', 'C')")

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
                "SELECT SCHEDBLOCKUID as SB_UID, QA0STATUS, STARTTIME, ENDTIME,"
                "EXECBLOCKUID, EXECFRACTION "
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
                "WHERE regexp_like (CYCLE, '^201[35].[1A]')")
            self.cursor.execute(sql2)
            self.executive = pd.DataFrame(
                self.cursor.fetchall(), columns=['OBSPROJECT_UID', 'EXEC'])

            self.sql3 = str(
                "SELECT obs1.ARCHIVE_UID, obs1.PRJ_REF, obs1.SB_NAME, "
                "obs1.STATUS, obs1.EXECUTION_COUNT "
                "FROM ALMA.BMMV_SCHEDBLOCK obs1, ALMA.BMMV_OBSPROJECT obs2 "
                "WHERE obs1.PRJ_REF = obs2.PRJ_ARCHIVE_UID "
                "AND regexp_like (obs2.PRJ_CODE, '^2015\..*\.[AST]')"
            )

            self.start_apa()

        self.sqlstates = str(
            "SELECT DOMAIN_ENTITY_STATE as SB_STATE,"
            "DOMAIN_ENTITY_ID as SB_UID,OBS_PROJECT_ID as OBSPROJECT_UID "
            "FROM ALMA.SCHED_BLOCK_STATUS")
        self.cursor.execute(self.sqlstates)
        self.sb_status = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        ).set_index('SB_UID', drop=False)

    def start_apa(self):

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

        df1 = df1.query(
            '(CYCLE in ["2015.1", "2015.A"]) or '
            '(CYCLE in ["2013.1", "2013.A"] and DC_LETTER_GRADE == "A")').copy()

        print(len(df1.query('PRJ_STATUS not in @status')))

        self.projects = pd.merge(
            df1.query('PRJ_STATUS not in @status'), self.executive,
            on='OBSPROJECT_UID'
        ).set_index('CODE', drop=False)

        timestamp = pd.Series(
            np.zeros(len(self.projects), dtype=object),
            index=self.projects.index)
        self.projects['timestamp'] = timestamp
        self.projects['xmlfile'] = self.projects.apply(
            lambda r: r['OBSPROJECT_UID'].replace('://', '___').replace(
                '/', '_') + '.xml', axis=1
        )
        self.projects['phase'] = self.projects.apply(
            lambda r: 'I' if r['PRJ_STATUS'] in phase_i_status else 'II',
            axis=1
        )

        self.read_obsproject(path=self.phase1_data + 'obsproject/')

    def read_obsproject(self, path):

        projt = []

        for r in self.projects.iterrows():
            xml = r[1].xmlfile
            try:
                obsparse = ObsProject(xml, path)
            except KeyError:
                print("Something went wrong while trying to parse %s" % xml)
                return 0

            proj = obsparse.get_info()
            projt.append(proj)

        projt_arr = np.array(projt, dtype=object)
        self.obsproject = pd.DataFrame(
            projt_arr,
            columns=['CODE', 'OBSPROJECT_UID', 'OBSPROPOSAL_UID',
                     'OBSREVIEW_UID', 'VERSION',
                     'NOTE', 'IS_CALIBRATION', 'IS_DDT']
        ).set_index('OBSPROJECT_UID', drop=False)

    def read_sciencegoals(self):

        sgt = []
        tart = []
        visitt = []
        temp_paramt = []
        sgspwt = []
        sgspsct = []

        for r in self.obsproject.iterrows():
            obsproject_uid = r[1].OBSPROJECT_UID
            obsproposal_uid = r[1].OBSPROPOSAL_UID
            if obsproject_uid is None:
                continue
            xml = obsproposal_uid.replace('://', '___').replace('/', '_')
            xml += '.xml'
            try:
                if self.projects.ix[r[1].CODE, 'phase'] == 'I':
                    obspropparse = ObsProposal(
                        xml, obsproject_uid, self.phase1_data + 'obsproposal/')
                    obspropparse.get_sg()
                else:
                    # print "Processing Phase II %s" % r[1].CODE
                    xml = obsproject_uid.replace('://', '___').replace(
                        '/', '_')
                    xml += '.xml'
                    obspropparse = ObsProject(
                        xml, self.phase1_data + 'obsproject/')
                    obspropparse.get_sg()
            except IOError:
                print("Something went wrong while trying to parse %s" % xml)
                continue

            sgt.extend(obspropparse.sciencegoals)
            tart.extend(obspropparse.sg_targets)
            if len(obspropparse.sg_specscan) > 0:
                sgspsct.extend(obspropparse.sg_specscan)
            if len(obspropparse.sg_specwindows) > 0:
                sgspwt.extend(obspropparse.sg_specwindows)
            if len(obspropparse.visits) > 0:
                visitt.extend(obspropparse.visits)
            if len(obspropparse.temp_param) > 0:
                temp_paramt.extend(obspropparse.temp_param)

        sgt_arr = np.array(sgt, dtype=object)
        tart_arr = np.array(tart, dtype=object)
        visitt_arr = np.array(visitt, dtype=object)
        temp_paramt_arr = np.array(temp_paramt, dtype=object)
        sgspwt_arr = np.array(sgspwt, dtype=object)
        sgspsct_arr = np.array(sgspsct, dtype=object)

        self.sciencegoals = pd.DataFrame(
            sgt_arr,
            columns=['SG_ID', 'OBSPROJECT_UID', 'OUS_ID', 'sg_name', 'band',
                     'estimatedTime', 'est12Time', 'estACATime',
                     'est7Time', 'eTPTime',
                     'AR', 'LAS', 'ARcor', 'LAScor', 'sensitivity',
                     'useACA', 'useTP', 'isTimeConstrained', 'repFreq',
                     'repFreq_spec', 'singleContFreq', 'isCalSpecial',
                     'isPointSource', 'polarization', 'isSpectralScan', 'type',
                     'hasSB', 'dummy', 'num_targets', 'mode']
        ).set_index('SG_ID', drop=False)

        self.sg_targets = pd.DataFrame(
            tart_arr,
            columns=['TARG_ID', 'OBSPROJECT_UID', 'SG_ID', 'tarType',
                     'solarSystem', 'sourceName', 'RA', 'DEC', 'isMosaic',
                     'centerVel', 'centerVel_units', 'centerVel_refsys',
                     'centerVel_doppler', 'lineWidth']
        ).set_index('TARG_ID', drop=False)

        self.visits = pd.DataFrame(
            visitt_arr,
            columns=['SG_ID', 'sgName', 'OBSPROJECT_UID', 'startTime', 'margin',
                     'margin_unit', 'note', 'avoidConstraint', 'priority',
                     'visit_id', 'prev_visit_id', 'requiredDelay',
                     'requiredDelay_unit', 'fixedStart']
        )

        self.temp_param = pd.DataFrame(
            temp_paramt_arr,
            columns=['SG_ID', 'sgName', 'OBSPROJECT_UID', 'startTime',
                     'endTime', 'margin', 'margin_unit', 'repeats', 'LSTmin',
                     'LSTmax', 'note', 'avoidConstraint', 'priority',
                     'fixedStart']
        )

        self.sg_spw = pd.DataFrame(
            sgspwt_arr,
            columns=['SG_ID', 'SPW_ID', 'transitionName', 'centerFrequency',
                     'bandwidth', 'spectralRes', 'isRepSPW', 'isSkyFreq',
                     'group_index']
        )
        self.sg_specscan = pd.DataFrame(
            sgspsct_arr,
            columns=['SG_ID', 'SSCAN_ID', 'startFrequency', 'endFrequency',
                     'bandwidth', 'spectralRes', 'isSkyFreq']
        )

    def read_sblocks(self):
        sbt = []
        for r in self.obsproject.iterrows():
            if self.projects.ix[r[1].CODE, 'phase'] == 'I':
                obsreview_uid = r[1].OBSREVIEW_UID
                if obsreview_uid is None:
                    continue
                xml = obsreview_uid.replace('://', '___').replace('/', '_')
                xml += '.xml'
                try:
                    obsrevparse = ObsReview(xml,
                                            self.phase1_data + 'obsreview/')
                except IOError:
                    print("Something went wrong while trying to parse %s" % xml)
                    continue
                obsrevparse.get_sg_sb()
                sbt.extend(obsrevparse.sg_sb)
            else:
                obsproject_uid = r[1].OBSPROJECT_UID
                xml = obsproject_uid.replace('://', '___').replace('/', '_')
                xml += '.xml'
                try:
                    obsproparse = ObsProject(xml,
                                             self.phase1_data + 'obsproject/')
                except IOError:
                    print("Something went wrong while trying to parse %s" % xml)
                    continue
                obsproparse.get_sg_sb()
                sbt.extend(obsproparse.sg_sb)

        sbt_arr = np.array(sbt, dtype=object)

        self.sblocks = pd.DataFrame(
            sbt_arr,
            columns=['SB_UID', 'OBSPROJECT_UID', 'ous_name', 'OUS_ID', 'GOUS_ID',
                     'gous_name', 'MOUS_ID', 'mous_name',
                     'array', 'execount']
        ).set_index('SB_UID', drop=False)

        self.sblocks['sg_name'] = self.sblocks.ous_name.str.replace(
            "SG OUS \(", "")
        self.sblocks['sg_name'] = self.sblocks.sg_name.str.slice(0, -1)

    def read_sb(self, path):

        rst = []
        rft = []
        tart = []
        spwt = []
        bbt = []
        spct = []
        scpart = []
        acpart = []
        bcpart = []
        pcpart = []
        ordtart = []
        sys.stdout.write("Processing Phase II SBs ")
        sys.stdout.flush()

        c = 10
        i = 0
        n = len(self.sblocks)
        for sg_sb in self.sblocks.iterrows():
            i += 1
            if (100. * i / n) > c:
                sys.stdout.write('.')
                sys.stdout.flush()
                c += 10

            xmlf = sg_sb[1].SB_UID.replace('://', '___')
            xmlf = xmlf.replace('/', '_') + '.xml'

            sb1 = SchedBlock(
                xmlf, sg_sb[1].SB_UID, sg_sb[1].OBSPROJECT_UID, sg_sb[1].OUS_ID,
                sg_sb[1].sg_name, path)

            rs, rf, tar, spc, bb, spw, scpar, acpar, bcpar, pcpar, ordtar = \
                sb1.read_schedblocks()
            rst.append(rs)
            rft.extend(rf)
            tart.extend(tar)
            spct.extend(spc)
            bbt.extend(bb)
            spwt.extend(spw)
            scpart.extend(scpar)
            acpart.extend(acpar)
            bcpart.extend(bcpar)
            pcpart.extend(pcpar)
            ordtart.extend(ordtar)

        sys.stdout.write("\nDone!\n")
        sys.stdout.flush()

        rst_arr = np.array(rst, dtype=object)
        rft_arr = np.array(rft, dtype=object)
        tart_arr = np.array(tart, dtype=object)
        spct_arr = np.array(spct, dtype=object)
        bbt_arr = np.array(bbt, dtype=object)
        spwt_arr = np.array(spwt, dtype=object)
        scpart_arr = np.array(scpart, dtype=object)
        acpart_arr = np.array(acpart, dtype=object)
        bcpart_arr = np.array(bcpart, dtype=object)
        pcpart_arr = np.array(pcpart, dtype=object)
        ordtart_arr = np.array(ordtart, dtype=object)

        self.schedblocks = pd.DataFrame(
            rst_arr,
            columns=['SB_UID', 'OBSPROJECT_UID', 'SG_ID', 'OUS_ID',
                     'sbName', 'sbNote', 'sbStatusXml', 'repfreq', 'band',
                     'array',
                     'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'execount',
                     'isPolarization', 'maxPWVC', 'array12mType',
                     'estimatedTime', 'maximumTime'],
        ).set_index('SB_UID', drop=False)

        tof = ['repfreq', 'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'maxPWVC']
        self.schedblocks[tof] = self.schedblocks[tof].astype(float)
        self.schedblocks[['execount']] = self.schedblocks[
            ['execount']].astype(int)

        self.scienceparam = pd.DataFrame(
            scpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'representative_bw',
                     'sensitivy', 'sensUnit', 'intTime', 'subScanDur']
        ).set_index('paramRef', drop=False)

        self.ampcalparam = pd.DataFrame(
            acpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.bbandcalparam = pd.DataFrame(
            bcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.phasecalparam = pd.DataFrame(
            pcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.orederedtar = pd.DataFrame(
            ordtart_arr,
            columns=['targetId', 'SB_UID', 'indObs', 'name']
        ).set_index('targetId', drop=False)

        self.fieldsource = pd.DataFrame(
            rft_arr,
            columns=['fieldRef', 'SB_UID', 'solarSystem', 'sourcename',
                     'name', 'RA', 'DEC', 'isQuery', 'intendedUse', 'qRA',
                     'qDEC', 'use', 'search_radius', 'rad_unit',
                     'ephemeris', 'pointings', 'isMosaic', 'arraySB']
        ).set_index('fieldRef', drop=False)

        self.target = pd.DataFrame(
            tart_arr,
            columns=['targetId', 'SB_UID', 'specRef', 'fieldRef',
                     'paramRef']).set_index('targetId', drop=False)

        self.spectralconf = pd.DataFrame(
            spct_arr,
            columns=['specRef', 'SB_UID', 'Name', 'BaseBands', 'SPWs']
        ).set_index('specRef', drop=False)

        self.spectralconf[['BaseBands', 'SPWs']] = self.spectralconf[
            ['BaseBands', 'SPWs']].astype(int)

        self.baseband = pd.DataFrame(
            bbt_arr,
            columns=['basebandRef', 'spectralConf', 'SB_UID', 'Name',
                     'CenterFreq', 'FreqSwitching', 'l02Freq',
                     'Weighting', 'useUDB']
        ).set_index('basebandRef', drop=False)

        tof = ['CenterFreq', 'l02Freq', 'Weighting']
        tob = ['FreqSwitching', 'useUDB']

        self.baseband[tof] = self.baseband[tof].astype(float)
        self.baseband[tob] = self.baseband[tob].astype(bool)

        tof = ['CenterFreq', 'EffectiveBandwidth', 'lineRestFreq']
        toi = ['AveragingFactor', 'EffectiveChannels']
        tob = ['Use']

        self.spectralwindow = pd.DataFrame(
            spwt_arr,
            columns=['basebandRef', 'SB_UID', 'Name',
                     'SideBand', 'WindowsFunction',
                     'CenterFreq', 'AveragingFactor',
                     'EffectiveBandwidth', 'EffectiveChannels', 'lineRestFreq',
                     'lineName', 'Use'],
        ).set_index('basebandRef', drop=False)

        self.spectralwindow[tof] = self.spectralwindow[tof].astype(float)
        self.spectralwindow[toi] = self.spectralwindow[toi].astype(int)
        self.spectralwindow[tob] = self.spectralwindow[tob].astype(bool)

    # noinspection PyUnusedLocal
    def get_ar_lim(self, sbrow):

        ouid = sbrow['OBSPROJECT_UID']
        sgn = sbrow['SG_ID']
        uid = sbrow['SB_UID']
        ousid = sbrow['OUS_ID']
        sgrow = self.sciencegoals.query('OBSPROJECT_UID == @ouid and '
                                        '(sg_name == @sgn or '
                                        ' OUS_ID == @ousid)')
        if len(sgrow) == 0:
            sgn = sbrow['SG_ID'].rstrip()
            sgn = sgn.lstrip()
            sgrow = self.sciencegoals.query(
                'OBSPROJECT_UID == @ouid and (sg_name == @sgn or '
                ' OUS_ID == @ousid)')

        if len(sgrow) == 0:
            if sbrow['SG_ID'] == 'CO(4-3), [CI] 1-0 setup':
                sgn = 'CO(4-3), [CI]1-0 setup'
            elif sbrow['SG_ID'] == 'CO(7-6), [CI] 2-1 setup':
                sgn = 'CO(7-6), [CI]2-1 setup'
            elif sbrow['SG_ID'] == 'CRL618: HNC & HCO+ 3-2 + H29a + HC3N 28-27':
                sgn = 'CRL618: HNC &HCO+ 3-2 + H29a + HC3N 28-27'
            elif sbrow['SG_ID'] == 'HCN, H13CN & HC15N J=8-7':
                sgn = 'HCN, H13CN &HC15N J=8-7'
            else:
                sgn = sgn
            sgrow = self.sciencegoals.query(
                'OBSPROJECT_UID == @ouid and (sg_name == @sgn or '
                ' OUS_ID == @ousid)')

        sbs = self.schedblocks.query(
            'OBSPROJECT_UID == @ouid and SG_ID == @sgn and array == "TWELVE-M"')
        isextended = True
        sb_bl_num = len(sbs)
        sb_7m_num = len(self.schedblocks.query(
            'OBSPROJECT_UID == @ouid and SG_ID == @sgn and array == "SEVEN-M"'))
        sb_tp_num = len(self.schedblocks.query(
            'OBSPROJECT_UID == @ouid and SG_ID == @sgn and '
            'array == "TP-Array"'))

        if sbrow['array'] != "TWELVE-M":
            return pd.Series(
                [None, None, 'N/A', 0, sb_bl_num, sb_7m_num, sb_tp_num],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num"])
        if len(sgrow) == 0:
            print "What? %s" % uid
            return pd.Series(
                [0, 0, 'E', 0, sb_bl_num, sb_7m_num, sb_tp_num],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num"])
        else:
            sgrow = sgrow.iloc[0]

        num12 = 1

        if len(sbs) > 1:
            two = sbs[sbs.array12mType.str.contains('Comp')]
            if len(two) > 0:
                num12 = 2
                isextended = True
                if sbrow['sbName'].endswith('_TC'):
                    isextended = False

        # noinspection PyBroadException
        try:
            minar, maxar, conf1, conf2 = self.ares.run(
                sgrow['ARcor'], sgrow['LAScor'], sbrow['DEC'], sgrow['useACA'],
                num12, sbrow['OT_BestConf'], uid)
        except:
            print "Exception, %s" % uid
            print sgrow['ARcor'], sgrow['LAScor'], sbrow['DEC'], sgrow['useACA']
            return pd.Series(
                [0, 0, 'C', num12, sb_bl_num, sb_7m_num, sb_tp_num],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num"])

        if not isextended:

            return pd.Series(
                [minar[1], maxar[1], conf2, num12, sb_bl_num, sb_7m_num,
                 sb_tp_num],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num"])

        return pd.Series(
            [minar[0], maxar[0], conf1, num12, sb_bl_num, sb_7m_num, sb_tp_num],
            index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                   "SB_7m_num", "SB_TP_num"])
