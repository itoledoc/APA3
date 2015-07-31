__author__ = 'itoledo'

from lxml import objectify
import pandas as pd
from converter3 import *
import ephem

prj = '{Alma/ObsPrep/ObsProject}'
val = '{Alma/ValueTypes}'
sbl = '{Alma/ObsPrep/SchedBlock}'


class ObsProposal(object):

    def __init__(self, xml_file, obsproject_uid, path='./'):
        """

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        self.obsproject_uid = obsproject_uid
        self.data = tree.getroot()

        self.sg_targets = []
        self.sciencegoals = []

    def get_ph1_sg(self):
        sg_list = self.data.findall(prj + 'ScienceGoal')
        c = 1
        for sg in sg_list:
            self.read_ph1_sg(sg, str("%02d" % c))
            c += 1

    def read_ph1_sg(self, sg, idnum):
        sg_id = self.obsproject_uid + '_' + str(idnum)

        # Handle exceptions of Science Goals without ObsUnitSets or SchedBlocks
        try:
            # ous_id = the id of associated ObsUnitSet at the ObsProgram level.
            ous_id = sg.ObsUnitSetRef.attrib['partId']
            hasSB = True
        except AttributeError:
            ous_id = None
            hasSB = False

        # get SG name, bands and estimated time
        sg_name = sg.name.pyval
        sg_mode = sg.attrib['mode']
        bands = sg.findall(prj + 'requiredReceiverBands')[0].pyval
        estimatedTime = convert_tsec(
            sg.estimatedTotalTime.pyval,
            sg.estimatedTotalTime.attrib['unit']) / 3600.

        # get SG's AR (in arcsec), LAS (arcsec), sensitivity (Jy), useACA and
        # useTP
        performance = sg.PerformanceParameters
        AR = convert_sec(
            performance.desiredAngularResolution.pyval,
            performance.desiredAngularResolution.attrib['unit'])
        LAS = convert_sec(
            performance.desiredLargestScale.pyval,
            performance.desiredLargestScale.attrib['unit'])
        sensitivity = convert_jy(
            performance.desiredSensitivity.pyval,
            performance.desiredSensitivity.attrib['unit'])
        useACA = performance.useACA.pyval
        useTP = performance.useTP.pyval
        isPointSource = performance.isPointSource.pyval

        # Check if it is a TimeConstrained SG
        try:
            isTimeConstrained = performance.isTimeConstrained.pyval
        except AttributeError:
            isTimeConstrained = None

        # Get SG representative Frequency, polarization configuration.
        spectral = sg.SpectralSetupParameters
        repFreq = convert_ghz(
            performance.representativeFrequency.pyval,
            performance.representativeFrequency.attrib['unit'])
        polarization = spectral.attrib['polarisation']
        type_pol = spectral.attrib['type']

        # Correct AR and LAS to equivalent resolutions at 100GHz
        ARcor = AR * repFreq / 100.
        LAScor = LAS * repFreq / 100.

        # set variables that will be filled later. The relevant variable to
        # calculate new minAR and maxAR is two_12m, False if needs one 12m conf.

        two_12m = None
        targets = sg.findall(prj + 'TargetParameters')
        num_targets = len(targets)
        c = 1
        for t in targets:
            self.read_pro_targets(t, sg_id, self.obsproject_uid, c)
            c += 1

        twelveTime = convert_tsec(
            sg.estimated12mTime.pyval,
            sg.estimated12mTime.attrib['unit']) / 3600.
        ACATime = convert_tsec(
            sg.estimatedACATime.pyval,
            sg.estimatedACATime.attrib['unit']) / 3600.
        sevenTime = convert_tsec(
            float(sg.estimated7mTime.pyval),
            sg.estimated7mTime.attrib['unit']) / 3600.
        TPTime = convert_tsec(
            sg.estimatedTPTime.pyval,
            sg.estimatedTPTime.attrib['unit']) / 3600.

        # Stores Science Goal parameters in a data frame instance

        self.sciencegoals.append([
            sg_id, self.obsproject_uid, ous_id, sg_name, bands, estimatedTime,
            twelveTime, ACATime, sevenTime, TPTime, AR, LAS, ARcor,
            LAScor, sensitivity, useACA, useTP, isTimeConstrained, repFreq,
            isPointSource, polarization, type_pol, hasSB, two_12m, num_targets,
            sg_mode]
        )

    def read_pro_targets(self, target, sgid, obsp_uid, c):

        tid = sgid + '_' + str(c)
        try:
            solarSystem = target.attrib['solarSystemObject']
        except KeyError:
            solarSystem = None

        typetar = target.attrib['type']
        sourceName = target.sourceName.pyval
        coord = target.sourceCoordinates
        coord_type = coord.attrib['system']
        if coord_type == 'J2000':
            ra = convert_deg(coord.findall(val + 'longitude')[0].pyval,
                             coord.findall(val + 'longitude')[0].attrib['unit'])
            dec = convert_deg(coord.findall(val + 'latitude')[0].pyval,
                              coord.findall(val + 'latitude')[0].attrib['unit'])
        elif coord_type == 'galactic':
            lon = convert_deg(
                coord.findall(val + 'longitude')[0].pyval,
                coord.findall(val + 'longitude')[0].attrib['unit'])
            lat = convert_deg(
                coord.findall(val + 'latitude')[0].pyval,
                coord.findall(val + 'latitude')[0].attrib['unit'])
            eph = ephem.Galactic(pd.np.radians(lon), pd.np.radians(lat))
            ra = pd.np.degrees(eph.to_radec()[0])
            dec = pd.np.degrees(eph.to_radec()[1])
        else:
            print "coord type is %s, deal with it" % coord_type
            ra = 0
            dec = 0
        try:
            isMosaic = target.isMosaic.pyval
        except AttributeError:
            isMosaic = None

        self.sg_targets.append(
            [tid, obsp_uid, sgid, typetar, solarSystem, sourceName, ra, dec,
             isMosaic])


class ObsProject(object):

    def __init__(self, xml_file, path='./'):
        """
        Notes
        -----

        ObsProject level:
        - isDDT may or may not be present, but we assume is unique.

        ScienceGoal level:
        - We only look for ScienceGoals. If a ScienceGoal has an ObsUnitSetRef
          then it might has SBs, but we assume there is only one OUSRef.
        - For the estimatedTotalTime, we assume it is the sum of the OT
          calculations, incluing ExtT, CompT, ACAT, TPT
        - Cycle 1 and 2 have only one band as requirement, supposely, but we
          check for more just in case.

        PerformanceParameters level:
        - Only one representativeFrequency assumed

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        data = tree.getroot()
        self.status = data.attrib['status']
        for key in data.__dict__:
            self.__setattr__(key, data.__dict__[key])

    def get_ph1_info(self):

        code = self.code.pyval
        prj_version = self.version.pyval
        staff_note = self.staffProjectNote.pyval
        is_calibration = self.isCalibration.pyval
        obsproject_uid = self.ObsProjectEntity.attrib['entityId']
        obsproposal_uid = self.ObsProposalRef.attrib['entityId']
        try:
            obsreview_uid = self.ObsReviewRef.attrib['entityId']
        except AttributeError:
            obsreview_uid = None

        try:
            is_ddt = self.isDDT.pyval
        except AttributeError:
            is_ddt = False

        return [code, obsproject_uid, obsproposal_uid, obsreview_uid,
                prj_version, staff_note, is_calibration, is_ddt]


class SchedBlock(object):

    def __init__(self, xml_file, sb_uid, obs_uid, ous_id, sg_id,
                 path='./'):
        """

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        self.data = tree.getroot()
        self.sb_uid = sb_uid
        self.ous_id = ous_id
        self.obs_uid = obs_uid
        self.sg_id = sg_id

    def read_schedblocks(self):

        # Open SB with SB parser class
        """

        :param sb_uid:
        :param new:
        """

        # Extract root level data
        array = self.data.findall(
            './/' + prj + 'ObsUnitControl')[0].attrib['arrayRequested']
        name = self.data.findall('.//' + prj + 'name')[0].pyval
        note = self.data.find('.//' + prj + 'note').pyval
        type12m = 'None'
        if name.rfind('TC') != -1:
            type12m = 'Comp'
        elif array == 'TWELVE-M':
            type12m = 'Ext'
        status = self.data.attrib['status']

        schedconstr = self.data.SchedulingConstraints
        schedcontrol = self.data.SchedBlockControl
        preconditions = self.data.Preconditions
        weather = preconditions.findall('.//' + prj + 'WeatherConstraints')[0]
        ouc = self.data.find('.//' + prj + 'ObsUnitControl')
        estimatedTimet = ouc.find('.//' + prj + 'estimatedExecutionTime')
        estimatedTime = convert_thour(estimatedTimet.pyval,
                                     estimatedTimet.attrib['unit'])
        maximumTimet = ouc.find('.//' + prj + 'maximumTime')
        maximumTime = convert_thour(maximumTimet.pyval,
                                   maximumTimet.attrib['unit'])

        try:
            # noinspection PyUnusedLocal
            polarparam = self.data.PolarizationCalParameters
            ispolarization = True
        except AttributeError:
            ispolarization = False

        repfreq = schedconstr.representativeFrequency.pyval
        ra = schedconstr.representativeCoordinates.findall(
            val + 'longitude')[0].pyval
        dec = schedconstr.representativeCoordinates.findall(
            val + 'latitude')[0].pyval
        minar_old = schedconstr.minAcceptableAngResolution.pyval
        maxar_old = schedconstr.maxAcceptableAngResolution.pyval
        band = schedconstr.attrib['representativeReceiverBand']

        execount = schedcontrol.executionCount.pyval
        maxpwv = weather.maxPWVC.pyval

        n_fs = len(self.data.FieldSource)
        n_tg = len(self.data.Target)
        n_ss = len(self.data.SpectralSpec)
        try:
            n_sp = len(self.data.ScienceParameters)
        except AttributeError:
            n_sp = 0
            print "\nWarning, %s is malformed" % self.sb_uid
        try:
            n_acp = len(self.data.AmplitudeCalParameters)
        except AttributeError:
            n_acp = 0

        try:
            n_bcp = len(self.data.BandpassCalParameters)
        except AttributeError:
            n_bcp = 0

        try:
            n_pcp = len(self.data.PhaseCalParameters)
        except AttributeError:
            n_pcp = 0

        try:
            n_ogroup = len(self.data.ObservingGroup)
        except AttributeError:
            n_ogroup = 0

        rf = []
        new = True
        for n in range(n_fs):
            if new:
                rf.append(self.read_fieldsource(
                    self.data.FieldSource[n], self.sb_uid, array))
                new = False
            else:
                rf.append(self.read_fieldsource(
                    self.data.FieldSource[n], self.sb_uid, array))

        tar = []
        new = True
        for n in range(n_tg):
            if new:
                tar.append(
                    self.read_target(self.data.Target[n], self.sb_uid))
                new = False
            else:
                tar.append(self.read_target(self.data.Target[n], self.sb_uid))

        spc = []
        bb = []
        spw = []
        new = True
        for n in range(n_ss):
            if new:
                r = self.read_spectralconf(self.data.SpectralSpec[n], self.sb_uid)
                spc.append(r[0])
                bb.extend(r[1])
                spw.extend(r[2])
                new = False
            else:
                r = self.read_spectralconf(self.data.SpectralSpec[n], self.sb_uid)
                spc.append(r[0])
                bb.extend(r[1])
                spw.extend(r[2])

        scpar = []
        for n in range(n_sp):
            sp = self.data.ScienceParameters[n]
            en_id = sp.attrib['entityPartId']
            namep = sp.name.pyval
            rep_bw = convert_ghz(sp.representativeBandwidth.pyval,
                                 sp.representativeBandwidth.attrib['unit'])
            sen_goal = sp.sensitivityGoal.pyval
            sen_goal_u = sp.sensitivityGoal.attrib['unit']
            int_time = convert_tsec(sp.integrationTime.pyval,
                                    sp.integrationTime.attrib['unit'])
            subs_dur = convert_tsec(sp.subScanDuration.pyval,
                                    sp.subScanDuration.attrib['unit'])
            scpar.append([en_id, self.sb_uid, namep, rep_bw, sen_goal, sen_goal_u,
                          int_time, subs_dur])

        acpar = []
        if n_acp > 0:
            for n in range(n_acp):
                sp = self.data.AmplitudeCalParameters[n]
                en_id = sp.attrib['entityPartId']
                namep = sp.name.pyval
                int_time = convert_tsec(
                    sp.defaultIntegrationTime.pyval,
                    sp.defaultIntegrationTime.attrib['unit'])
                subs_dur = convert_tsec(sp.subScanDuration.pyval,
                                        sp.subScanDuration.attrib['unit'])
                acpar.append([en_id, self.sb_uid, namep, int_time, subs_dur])

        bcpar = []
        if n_bcp > 0:
            for n in range(n_bcp):
                sp = self.data.BandpassCalParameters[n]
                en_id = sp.attrib['entityPartId']
                namep = sp.name.pyval
                int_time = convert_tsec(
                    sp.defaultIntegrationTime.pyval,
                    sp.defaultIntegrationTime.attrib['unit'])
                subs_dur = convert_tsec(sp.subScanDuration.pyval,
                                        sp.subScanDuration.attrib['unit'])
                bcpar.append([en_id, self.sb_uid, namep, int_time, subs_dur])

        pcpar = []
        if n_pcp > 0:
            for n in range(n_pcp):
                sp = self.data.PhaseCalParameters[n]
                en_id = sp.attrib['entityPartId']
                namep = sp.name.pyval
                int_time = convert_tsec(
                    sp.defaultIntegrationTime.pyval,
                    sp.defaultIntegrationTime.attrib['unit'])
                subs_dur = convert_tsec(sp.subScanDuration.pyval,
                                        sp.subScanDuration.attrib['unit'])
                pcpar.append([en_id, self.sb_uid, namep, int_time, subs_dur])

        ordtar = []
        if n_ogroup > 0:
            for n in range(n_ogroup):
                ogroup = self.data.ObservingGroup[n]
                nameo = ogroup.name.pyval
                try:
                    n_otar = len(ogroup.OrderedTarget)
                except AttributeError:
                    continue
                if n_otar > 0:
                    for o in range(n_otar):
                        otar = ogroup.OrderedTarget[o]
                        index = 0
                        tar_ref = otar.TargetRef.attrib['partId']
                        ordtar.append([tar_ref, self.sb_uid, index, nameo])
                else:
                    continue

        return (self.sb_uid, self.obs_uid, self.sg_id, self.ous_id,
                name, note, status, float(repfreq), band, array,
                float(ra), float(dec), float(minar_old), float(maxar_old),
                int(execount), ispolarization, float(maxpwv),
                type12m, estimatedTime, maximumTime
                ), rf, tar, spc, bb, spw, scpar, acpar, bcpar, pcpar, ordtar

    @staticmethod
    def read_fieldsource(fs, sbuid, array):
        """

        :param fs:
        :param sbuid:
        """
        partid = fs.attrib['entityPartId']
        coord = fs.sourceCoordinates
        solarsystem = fs.attrib['solarSystemObject']
        sourcename = fs.sourceName.pyval
        name = fs.name.pyval
        isquery = fs.isQuery.pyval
        pointings = len(fs.findall(sbl + 'PointingPattern/' + sbl +
                                   'phaseCenterCoordinates'))
        try:
            ismosaic = fs.PointingPattern.isMosaic.pyval
        except AttributeError:
            ismosaic = False
        if isquery:
            querysource = fs.QuerySource
            qc_intendeduse = querysource.attrib['intendedUse']
            qcenter = querysource.queryCenter
            qc_ra = qcenter.findall(val + 'longitude')[0].pyval
            qc_dec = qcenter.findall(val + 'latitude')[0].pyval
            qc_use = querysource.use.pyval
            qc_radius = querysource.searchRadius.pyval
            qc_radius_unit = querysource.searchRadius.attrib['unit']
        else:
            qc_intendeduse, qc_ra, qc_dec, qc_use, qc_radius, qc_radius_unit = (
                None, None, None, None, None, None
            )
        coord_type = coord.attrib['system']
        if coord_type == 'J2000':
            ra = convert_deg(coord.findall(val + 'longitude')[0].pyval,
                             coord.findall(val + 'longitude')[0].attrib['unit'])
            dec = convert_deg(coord.findall(val + 'latitude')[0].pyval,
                              coord.findall(val + 'latitude')[0].attrib['unit'])
        elif coord_type == 'galactic':
            lon = convert_deg(
                coord.findall(val + 'longitude')[0].pyval,
                coord.findall(val + 'longitude')[0].attrib['unit'])
            lat = convert_deg(
                coord.findall(val + 'latitude')[0].pyval,
                coord.findall(val + 'latitude')[0].attrib['unit'])
            eph = ephem.Galactic(pd.np.radians(lon), pd.np.radians(lat))
            ra = pd.np.degrees(eph.to_radec()[0])
            dec = pd.np.degrees(eph.to_radec()[1])
        else:
            print "coord type is %s, deal with it" % coord_type
            ra = 0
            dec = 0
        if solarsystem == 'Ephemeris':
            ephemeris = fs.sourceEphemeris.pyval
        else:
            ephemeris = None

        return(
            partid, sbuid, solarsystem, sourcename, name, ra, dec, isquery,
            qc_intendeduse, qc_ra, qc_dec, qc_use, qc_radius, qc_radius_unit,
            ephemeris, pointings, ismosaic, array)

    @staticmethod
    def read_target(tg, sbuid):
        """

        :param tg:
        :param sbuid:
        """
        partid = tg.attrib['entityPartId']
        specref = tg.AbstractInstrumentSpecRef.attrib['partId']
        fieldref = tg.FieldSourceRef.attrib['partId']
        paramref = tg.ObservingParametersRef.attrib['partId']

        return (
            partid, sbuid, specref, fieldref, paramref)

    def read_spectralconf(self, ss, sbuid):
        """

        :param ss:
        :param sbuid:
        """

        name = ss.name.pyval
        partid = ss.attrib['entityPartId']
        freqconf = ss.FrequencySetup
        bb = self.read_baseband(partid, freqconf, sbuid)

        try:
            corrconf = ss.BLCorrelatorConfiguration
            spw = self.read_spectralwindow(corrconf, sbuid)
            nbb = len(corrconf.BLBaseBandConfig)
            nspw = 0
            for n in range(nbb):
                bbconf = corrconf.BLBaseBandConfig[n]
                nspw += len(bbconf.BLSpectralWindow)
        except AttributeError:
            corrconf = ss.ACACorrelatorConfiguration
            spw = self.read_spectralwindow(corrconf, sbuid)
            nbb = len(corrconf.ACABaseBandConfig)
            nspw = 0
            for n in range(nbb):
                bbconf = corrconf.ACABaseBandConfig[n]
                nspw += len(bbconf.ACASpectralWindow)

        spc = (partid, sbuid, name, nbb, nspw)

        return spc, bb, spw

    @staticmethod
    def read_baseband(spectconf, freqconf, sbuid):
        bbl = []
        rest_freq = convert_ghz(freqconf.restFrequency.pyval,
                                freqconf.restFrequency.attrib['unit'])
        trans_name = freqconf.transitionName.pyval
        lo1_freq = convert_ghz(freqconf.lO1Frequency.pyval,
                               freqconf.lO1Frequency.attrib['unit'])
        band = freqconf.attrib['receiverBand']
        doppler_ref = freqconf.attrib['dopplerReference']
        for baseband in range(len(freqconf.BaseBandSpecification)):
            bb = freqconf.BaseBandSpecification[baseband]
            partid = bb.attrib['entityPartId']
            name = bb.attrib['baseBandName']
            center_freq_unit = bb.centerFrequency.attrib['unit']
            center_freq = convert_ghz(bb.centerFrequency.pyval,
                                      center_freq_unit)
            freq_switching = bb.frequencySwitching.pyval
            lo2_freq_unit = bb.lO2Frequency.attrib['unit']
            lo2_freq = convert_ghz(bb.lO2Frequency.pyval, lo2_freq_unit)
            weighting = bb.weighting.pyval
            use_usb = bb.useUSB.pyval
            bbl.append((partid, spectconf, sbuid, name, center_freq,
                        freq_switching, lo2_freq, weighting, use_usb))

        return bbl

    @staticmethod
    def read_spectralwindow(correconf, sbuid):
        spwl = []
        try:
            for baseband in range(len(correconf.BLBaseBandConfig)):
                bb = correconf.BLBaseBandConfig[baseband]
                bbRef = bb.BaseBandSpecificationRef.attrib['partId']
                for sw in range(len(bb.BLSpectralWindow)):
                    spw = bb.BLSpectralWindow[sw]
                    poln_prod = spw.attrib['polnProducts']
                    sideBand = spw.attrib['sideBand']
                    windowsFunction = spw.attrib['windowFunction']
                    name = spw.name.pyval
                    centerFreq_unit = spw.centerFrequency.attrib['unit']
                    centerFreq = convert_ghz(
                        spw.centerFrequency.pyval, centerFreq_unit)
                    averagingFactor = spw.spectralAveragingFactor.pyval
                    effectiveBandwidth_unit = spw.effectiveBandwidth.attrib[
                        'unit']
                    effectiveBandwidth = convert_ghz(
                        spw.effectiveBandwidth.pyval, effectiveBandwidth_unit)
                    effectiveChannels = spw.effectiveNumberOfChannels.pyval
                    use = spw.useThisSpectralWindow.pyval

                    spwl.append(
                        (bbRef, sbuid, name, sideBand, windowsFunction,
                         centerFreq, averagingFactor, effectiveBandwidth,
                         effectiveChannels, use))

        except AttributeError:
            for baseband in range(len(correconf.ACABaseBandConfig)):
                bb = correconf.ACABaseBandConfig[baseband]
                bbRef = bb.BaseBandSpecificationRef.attrib['partId']
                for sw in range(len(bb.ACASpectralWindow)):
                    spw = bb.ACASpectralWindow[sw]
                    sideBand = spw.attrib['sideBand']
                    windowsFunction = spw.attrib['windowFunction']
                    name = spw.name.pyval
                    centerFreq_unit = spw.centerFrequency.attrib['unit']
                    centerFreq = convert_ghz(
                        spw.centerFrequency.pyval, centerFreq_unit)
                    averagingFactor = spw.spectralAveragingFactor.pyval
                    effectiveBandwidth_unit = spw.effectiveBandwidth.attrib[
                        'unit']
                    effectiveBandwidth = convert_ghz(
                        spw.effectiveBandwidth.pyval, effectiveBandwidth_unit)
                    effectiveChannels = spw.effectiveNumberOfChannels.pyval
                    use = spw.useThisSpectralWindow.pyval
                    spwl.append(
                        (bbRef, sbuid, name, sideBand, windowsFunction,
                         centerFreq, averagingFactor, effectiveBandwidth,
                         effectiveChannels, use))
        return spwl


class ObsReview(object):

    def __init__(self, xml_file, path='./'):
        """

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        self.data = tree.getroot()
        self.sg_sb = []

    def get_sg_sb(self):

        oplan = self.data.find('{Alma/ObsPrep/ObsProject}' + 'ObsPlan')
        oussg_list = oplan.findall(prj + 'ObsUnitSet')
        for oussg in oussg_list:
            groupous_list = oussg.findall(prj + 'ObsUnitSet')
            OUS_ID = oussg.attrib['entityPartId']
            ous_name = oussg.name.pyval
            OBSPROJECT_UID = oussg.ObsProjectRef.attrib['entityId']

            # Now we iterate over all the children OUS (group ous and member
            # ous) until we find "SchedBlockRef" tags at the member ous
            # level.
            for groupous in groupous_list:
                gous_id = groupous.attrib['entityPartId']
                mous_list = groupous.findall(prj + 'ObsUnitSet')
                gous_name = groupous.name.pyval
                for mous in mous_list:
                    mous_id = mous.attrib['entityPartId']
                    mous_name = mous.name.pyval
                    try:
                        # TODO: WARNING, we are assuming that all mous have
                        # only one SchedBlockRef. For Cycle 1 and 2 this is
                        # Ok, but is not a correct assumption for future
                        sblist = mous.findall(prj + 'ObsUnitSet')
                        SB_UID = mous.SchedBlockRef.attrib['entityId']
                    except AttributeError:
                        continue
                    oucontrol = mous.ObsUnitControl
                    execount = oucontrol.aggregatedExecutionCount.pyval
                    array = mous.ObsUnitControl.attrib['arrayRequested']
                    for sbs in mous.SchedBlockRef:
                        SB_UID = sbs.attrib['entityId']
                        self.sg_sb.append(
                            [SB_UID, OBSPROJECT_UID, ous_name,
                             gous_id, gous_name, mous_id, mous_name, array,
                             execount])
