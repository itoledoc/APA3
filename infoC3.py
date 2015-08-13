import DataBase3 as db
import ephem
import numpy as np
import pylab as py

# dreload(db)

datas = db.Database()
datas.read_obspropsal_p1(datas.phase1_data + 'obsproposal/')
datas.obs_review(datas.phase1_data + 'obsreview/')
datas.sciencegoals['sg_name'] = datas.sciencegoals.sg_name.astype(str)
datas.read_sb(datas.phase1_data + 'schedblock/')
datas.sciencegoals.ix['uid://A001/X1ed/X38e_01', 'sg_name'] = '08477'
ar = datas.schedblocks.apply(lambda x: datas.get_ar_lim(x), axis=1)

import pandas as pd
grades = pd.read_excel(
    '/home/itoledo/Downloads/'
    'APRC_Meeting_Tool_assessments_list_20150723_UT1901.xlsx')

schedblocks = pd.concat([datas.schedblocks, ar], axis=1)
datas.baseband.columns = pd.Index(
    [u'basebandRef', u'specRef', u'SB_UID', u'Name', u'CenterFreq',
     u'FreqSwitching', u'l02Freq', u'Weighting', u'useUDB'], dtype='object')

sp_bb = pd.merge(
    datas.spectralconf, datas.baseband,
    on=['SB_UID', 'specRef'], how='left',
    suffixes=['_specconf', '_bbconf'])

spectral = pd.merge(
    sp_bb, datas.spectralwindow,
    on=['SB_UID', 'basebandRef'], how='left', suffixes=['_bbconf', '_spwconf'])
spectral.columns = pd.Index(
    [u'specRef', u'SB_UID', u'Name_specconf', u'BaseBands', u'SPWs',
     u'basebandRef', u'Name_bbconf', u'CenterFreq_bbconf', u'FreqSwitching',
     u'l02Freq', u'Weighting', u'useUDB', u'Name', u'SideBand',
     u'WindowsFunction', u'CenterFreq_spwconf', u'AveragingFactor',
     u'EffectiveBandwidth', u'EffectiveChannels', u'lineRestFreq',
     u'lineName', u'Use'], dtype='object')

spec_group = spectral.groupby(['SB_UID', 'specRef'])
spectral_agg = spec_group.agg(
    {'BaseBands': pd.np.max, 'SPWs': pd.np.max, 'CenterFreq_bbconf': pd.np.max,
     'CenterFreq_spwconf': pd.np.max, 'EffectiveBandwidth': pd.np.sum,
     'EffectiveChannels': pd.np.sum})

spectral_agg.columns = pd.Index(
    [u'CenterFreqMax_spwconf', u'BaseBands', u'EffectiveBandwidth_agg',
     u'EffectiveChannels_agg', u'SPWs', u'CenterFreqMax_bbconf'],
    dtype='object')

spectral_agg2 = spec_group.agg(
    {'EffectiveBandwidth': pd.np.min, 'EffectiveChannels': pd.np.min})
spectral_agg2.columns = pd.Index(
    ['EffectiveBandwidth_MIN', u'EffectiveChannels_MIN'], dtype='object')

spectral_agg = pd.merge(spectral_agg, spectral_agg2, left_index=True,
                        right_index=True)

spectral_agg3 = spec_group.agg(
    {'EffectiveBandwidth': pd.np.max, 'EffectiveChannels': pd.np.max})
spectral_agg3.columns = pd.Index(
    ['EffectiveBandwidth_MAX', u'EffectiveChannels_MAX'], dtype='object')

spectral_agg = pd.merge(
    spectral_agg, spectral_agg3, left_index=True, right_index=True)

scitar = pd.merge(
    datas.orederedtar.query('name != "Calibrators"'),
    datas.target, on=['SB_UID', 'targetId'])
scitar2 = pd.merge(
    scitar, datas.scienceparam,
    on=['SB_UID', 'paramRef'])
scitar2 = pd.merge(
    scitar2,
    datas.fieldsource[
        ['SB_UID', 'fieldRef', 'name', 'RA', 'DEC', 'isQuery', 'use',
         'solarSystem', 'isMosaic', 'pointings', 'ephemeris']
    ], on=['SB_UID', 'fieldRef'],
    suffixes=['_target', '_so'])
scitar3 = pd.merge(
    scitar2, spectral_agg.reset_index(), on=['SB_UID', 'specRef'])
scitar3_max = scitar3.groupby(['SB_UID', 'specRef']).agg(
    {'BaseBands': pd.np.max, 'SPWs': pd.np.max, 'CenterFreqMax_bbconf':
        pd.np.max, 'EffectiveBandwidth_agg': pd.np.median,
     'EffectiveChannels_agg': pd.np.median, 'EffectiveBandwidth_MIN': pd.np.min,
     'EffectiveChannels_MIN': pd.np.min, 'EffectiveBandwidth_MAX': pd.np.max,
     'EffectiveChannels_MAX': pd.np.max})

scitar3 = scitar3_max[
    ['BaseBands', 'SPWs', 'EffectiveChannels_agg',
     'EffectiveBandwidth_agg']].copy()

scitar3_SBmin = scitar3.reset_index().groupby('SB_UID').agg(
    {'specRef': pd.np.count_nonzero, 'BaseBands': pd.np.min, 'SPWs': pd.np.min,
     'EffectiveChannels_agg': pd.np.min, 'EffectiveBandwidth_agg': pd.np.min})
scitar3_SBmax = scitar3.reset_index().groupby('SB_UID').agg(
    {'BaseBands': pd.np.max, 'SPWs': pd.np.max,
     'EffectiveChannels_agg': pd.np.max, 'EffectiveBandwidth_agg': pd.np.max})

sb_speconf_agg = pd.merge(
    scitar3_SBmin, scitar3_SBmax, left_index=True, right_index=True,
    suffixes=['_MIN', '_MAX'])
sb_speconf_agg.columns= pd.Index(
    [u'EffectiveChannels_agg_MIN', u'spectral_setups', u'BaseBands_MIN',
     u'EffectiveBandwidth_agg_MIN', u'SPWs_MIN', u'EffectiveChannels_agg_MAX',
     u'SPWs_MAX', u'BaseBands_MAX', u'EffectiveBandwidth_agg_MAX'],
    dtype='object')



schedblocks = pd.merge(
    schedblocks, sb_speconf_agg.reset_index(), on='SB_UID')

merge1 = pd.merge(
    datas.projects,
    grades[['CODE', 'SCICAT', 'BANDS', 'time12m_aprc', 'time7m_aprc',
            'timeTP_aprc', 'timeNonStd_aprc', 'APRCSCORE', 'APRCNSCORE',
            'APRCRank', 'APRCGrade']],
    on='CODE')

merge2 = pd.merge(
    merge1,
    datas.obsproject_p1[['CODE', 'OBSREVIEW_UID']], on='CODE')

merge2.to_csv('/home/itoledo/Documents/project_info.csv')


merge3 = pd.merge(merge2, datas.sciencegoals, on=['OBSPROJECT_UID'])

merge3.to_csv('/home/itoledo/Documents/proj_sg.csv')

merge4 = pd.merge(
    merge3, schedblocks,
    left_on=['OBSPROJECT_UID', 'sg_name'],
    right_on=['OBSPROJECT_UID', 'SG_ID'], suffixes=['_SG', '_SB'])

tp_amp_su = merge4[merge4.sbNote.str.contains('mpca')].SB_UID.unique()

sb_fs = scitar2.query('SB_UID not in @tp_amp_su')
sb_fs_spw = pd.merge(scitar2, spectral, on=['SB_UID', 'specRef'])

sb_target_num = sb_fs.groupby('SB_UID').agg(
    {'fieldRef': pd.Series.nunique, 'pointings': pd.Series.max,
     'targetId': pd.Series.nunique, 'paramRef': pd.Series.nunique,
     'specRef': pd.Series.nunique}).reset_index()
single_field_su = sb_target_num.query('fieldRef == 1').SB_UID.unique()
schedblocks['single_field'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in single_field_su else False, axis=1)

single_point_su = sb_target_num.query('pointings <= 1').SB_UID.unique()
schedblocks['single_point'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in single_point_su else False, axis=1)

multi_point_su = sb_target_num.query('pointings >= 20').SB_UID.unique()
schedblocks['multi_point20'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in multi_point_su else False, axis=1)

multi_field_su = sb_target_num.query('fieldRef >= 15').SB_UID.unique()
schedblocks['multi_field15'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in multi_field_su else False, axis=1)

single_target_su = sb_target_num.query('targetId == 1').SB_UID.unique()
schedblocks['single_target'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in single_target_su else False, axis=1)

single_specconf_su = sb_target_num.query('specRef == 1').SB_UID.unique()
schedblocks['single_specconf'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in single_specconf_su else False, axis=1)

tdm_su = sb_fs_spw.groupby(
    'SB_UID').EffectiveChannels.max().reset_index().query(
    'EffectiveChannels <= 128').SB_UID.unique()
schedblocks['tdm_only'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in tdm_su else False, axis=1)

mixed_su = sb_fs_spw.groupby(
    'SB_UID').EffectiveChannels.agg(
    [pd.np.min, pd.np.max]).reset_index().query(
    'amin <= 128 and amax > 257').SB_UID.unique()
schedblocks['mixed_corr'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in mixed_su else False, axis=1)

ephem_su = sb_fs.query('solarSystem != "Unspecified"').SB_UID.unique()
schedblocks['ephem'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in ephem_su else False, axis=1)

ephem_int_su = sb_fs.query(
    'solarSystem != "Unspecified"')[
    sb_fs.query('solarSystem != "Unspecified"').ephemeris.isnull() == False
].SB_UID.unique()
schedblocks['ephem_int'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in ephem_int_su else False, axis=1)

has_cont_su = sb_fs_spw[
    (sb_fs_spw.lineName.str.contains('cont') == True) |
    (sb_fs_spw.lineName.str.contains('Cont') == True)].SB_UID.unique()
schedblocks['has_cont_only'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in has_cont_su else False, axis=1)

has_narrow_spw = sb_fs_spw.groupby(
    'SB_UID').EffectiveBandwidth.agg(
    [pd.np.min, pd.np.max]).reset_index().query('amin < 1').SB_UID.unique()
schedblocks['has_narrow_spw'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in has_narrow_spw else False, axis=1)

has_only_narrow_spw = sb_fs_spw.groupby(
    'SB_UID').EffectiveBandwidth.agg(
    [pd.np.min, pd.np.max]).reset_index().query('amax < 1').SB_UID.unique()
schedblocks['has_only_narrow_spw'] = schedblocks.apply(
    lambda x: True if x['SB_UID'] in has_only_narrow_spw else False, axis=1)

merge4 = pd.merge(
    merge3, schedblocks,
    left_on=['OBSPROJECT_UID', 'sg_name'],
    right_on=['OBSPROJECT_UID', 'SG_ID'], suffixes=['_SG', '_SB'])

merge4.to_csv('/home/itoledo/Documents/prj_sg_sb.csv')

alma1 = ephem.Observer()
alma1.lat = '-23.0262015'
alma1.long = '-67.7551257'
alma1.elev = 5060
alma1.horizon = ephem.degrees(str('20'))

import cycle3_tools as ct
data_ar = ct.create_dates(ct.es_cycle3)
date_df = pd.DataFrame(
    np.array(data_ar),
    columns=['start', 'end', 'block', 'C36_1', 'C36_2', 'C36_3', 'C36_4',
             'C36_5', 'C36_6', 'C36_7', 'C36_8'])
lst_times = date_df.apply(
    lambda r: ct.day_night(r['start'], r['end'], alma1), axis=1)

date_df = pd.concat([date_df, lst_times], axis=1)
date_df['available_time'] = date_df.apply(
    lambda r: (r['end'] - r['start']).total_seconds() / 3600., axis=1)
obs_param = merge4.apply(
    lambda r: ct.observable(
        r['RA'], r['DEC'], alma1, r['ephem'], r['minAR'], r['maxAR'],
        r['array'], r['SB_UID']), axis=1)
merge5 = pd.merge(merge4, obs_param, on='SB_UID', how='left')

availability = merge5.apply(
    lambda r: ct.avail_calc(
        r['rise'], r['set'], r['C36_1'], r['C36_2'], r['C36_3'], r['C36_4'],
        r['C36_5'], r['C36_6'], r['C36_7'], r['C36_8'], r['up'],
        r['band_SB'], date_df), axis=1)
summary = pd.concat([merge5, availability], axis=1)
summary.to_pickle('/home/itoledo/Documents/cycle3sum.data')
# summary = pd.read_pickle('/home/itoledo/Documents/cycle3sum.data')
summary['assumedconf_ar_ot'] = (summary.minAR_ot / 0.9) * summary.repFreq / 100.

c1 = np.sqrt(datas.ares.data[0][1] * datas.ares.data[0][2])
c2 = np.sqrt(datas.ares.data[1][1] * datas.ares.data[1][2])
c3 = np.sqrt(datas.ares.data[2][1] * datas.ares.data[2][2])
c4 = np.sqrt(datas.ares.data[3][1] * datas.ares.data[3][2])
c5 = np.sqrt(datas.ares.data[4][1] * datas.ares.data[4][2])
c6 = np.sqrt(datas.ares.data[5][1] * datas.ares.data[5][2])
c7 = np.sqrt(datas.ares.data[6][1] * datas.ares.data[6][2])
c8 = np.sqrt(datas.ares.data[7][1] * datas.ares.data[7][2])


def find_nearest(array, value):
    idx = (np.abs(array-value)).argmin()
    return array[idx], idx, (np.abs(array-value)).min()


def find_array(value):
    closest = 90000000.
    n = 0
    array = -20
    for c in [c1, c2, c3, c4, c5, c6, c7, c8]:
        a = find_nearest(c, value)
        if a[2] < closest:
            closest = a[2]
            array = n
        n += 1

    return datas.ares.array[array]

summary['OT_BestConf'] = summary.apply(
    lambda x: find_array(x['assumedconf_ar_ot']) if x['array'] == "TWELVE-M"
    else "N/A",
    axis=1)


datas.sg_targets.to_csv('/home/itoledo/Documents/sg_targets.csv')
datas.target.to_csv('/home/itoledo/Documents/sb_target.csv')
datas.orederedtar.to_csv('/home/itoledo/Documents/sb_order_target.csv')
datas.scienceparam.to_csv('/home/itoledo/Documents/sb_sciparam.csv')
datas.baseband.to_csv('/home/itoledo/Documents/sb_basebands.csv')
datas.spectralconf.to_csv('/home/itoledo/Documents/sb_specconf.csv')
datas.spectralwindow.to_csv('/home/itoledo/Documents/sb_specwindow.csv')
datas.fieldsource.to_csv('/home/itoledo/Documents/sb_fieldsource.csv')


# def av_arrays_opt(sel):
#
#     map_ra = np.arange(0, 24, 24. / (24 * 60.))
#     use_ra = np.zeros(1440)
#
#     for r in sel.iterrows():
#         data = r[1]
#         if data.RA == 0 or data.up == 24:
#             use_ra += (data.estimatedTime_SB / 24.) / data.twelve_good
#         elif data.rise > data.set:
#             use_ra[map_ra < data.set] += \
#                 (data.estimatedTime_SB / data.up) / data.twelve_good
#             use_ra[map_ra > data.rise] += \
#                 (data.estimatedTime_SB / data.up) / data.twelve_good
#         else:
#             use_ra[(map_ra > data.rise) & (map_ra < data.set)] += \
#                 (data.estimatedTime_SB / data.up) / data.twelve_good
#
#     return map_ra, use_ra
#
#
# def av_arrays(sel, minlst=-2., maxlst=2.):
#
#     map_ra = np.arange(0, 24, 24. / (24 * 60.))
#     use_ra_b3 = np.zeros(1440)
#     use_ra_b4 = np.zeros(1440)
#     use_ra_b6 = np.zeros(1440)
#     use_ra_b7 = np.zeros(1440)
#     use_ra_b8 = np.zeros(1440)
#     use_ra_b9 = np.zeros(1440)
#     use_ra_b10 = np.zeros(1440)
#
#     for r in sel.iterrows():
#         data = r[1]
#
#         if data.RA == 0 or data.up == 24:
#             use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, \
#                 use_ra_b10 = add_band(
#                     'all', data.estimatedTime_SB / 24., data.band_SB, use_ra_b3,
#                     use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9,
#                     use_ra_b10)
#             continue
#
#         if data.up > maxlst - minlst:
#             if maxlst < (data.RA / 15.) < 24 + minlst:
#                 rise = (data.RA / 15.) + minlst
#                 set = (data.RA / 15.) + maxlst
#                 up = maxlst - minlst
#                 use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, \
#                     use_ra_b9, use_ra_b10 = add_band(
#                         (map_ra > rise) & (map_ra < set),
#                         data.estimatedTime_SB / up, data.band_SB, use_ra_b3,
#                         use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9,
#                         use_ra_b10)
#             else:
#                 if (data.RA / 15.) < maxlst:
#                     rise = 24 + minlst + (data.RA / 15.)
#                     set = (data.RA / 15.) + maxlst
#                     up = maxlst - minlst
#                 else:
#                     rise = (data.RA / 15.) + minlst
#                     set = maxlst - (24 - (data.RA / 15.))
#                     up = maxlst - minlst
#                 use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, \
#                     use_ra_b9, use_ra_b10 = add_band(
#                         map_ra < set, data.estimatedTime_SB / up, data.band_SB,
#                         use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8,
#                         use_ra_b9, use_ra_b10)
#                 use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, \
#                     use_ra_b9, use_ra_b10 = add_band(
#                         map_ra > rise, data.estimatedTime_SB / up, data.band_SB,
#                         use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8,
#                         use_ra_b9, use_ra_b10)
#             continue
#
#         if data.rise > data.set:
#             rise = data.rise
#             set = data.set
#             up = data.up
#             if data.up > maxlst - minlst:
#                 if set > maxlst:
#                     set = maxlst
#                 if rise < 24 + minlst:
#                     rise = 24 + minlst
#                 up = maxlst - minlst
#             use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9,\
#                 use_ra_b10 = add_band(
#                     map_ra < set, data.estimatedTime_SB / up, data.band_SB,
#                     use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8,
#                     use_ra_b9, use_ra_b10)
#             use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, \
#                 use_ra_b10 = add_band(
#                     map_ra > rise, data.estimatedTime_SB / up, data.band_SB,
#                     use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8,
#                     use_ra_b9, use_ra_b10)
#         else:
#             rise = data.rise
#             set = data.set
#             up = data.up
#             if up > maxlst - minlst and data.ephem is False:
#                 if rise < data.RA / 15. + minlst:
#                     rise = data.RA / 15. + minlst
#                 if set > data.RA / 15. + maxlst:
#                     set = data.RA / 15. + maxlst
#                 up = maxlst - minlst
#                 use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, \
#                     use_ra_b9, use_ra_b10 = add_band(
#                         (map_ra > rise) & (map_ra < set),
#                         data.estimatedTime_SB / up, data.band_SB, use_ra_b3,
#                         use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9,
#                         use_ra_b10)
#
#     return map_ra, [use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8,
#                     use_ra_b9, use_ra_b10]
#
#
# def add_band(ind, timet, band, use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10):
#
#     if band == "ALMA_RB_03":
#         if ind == "all":
#             use_ra_b3 += timet
#         else:
#             use_ra_b3[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#     if band == "ALMA_RB_04":
#         if ind == "all":
#             use_ra_b4 += timet
#         else:
#             use_ra_b4[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#     if band == "ALMA_RB_06":
#         if ind == "all":
#             use_ra_b6 += timet
#         else:
#             use_ra_b6[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#     if band == "ALMA_RB_07":
#         if ind == "all":
#             use_ra_b7 += timet
#         else:
#             use_ra_b7[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#     if band == "ALMA_RB_08":
#         if ind == "all":
#             use_ra_b8 += timet
#         else:
#             use_ra_b8[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#     if band == "ALMA_RB_09":
#         if ind == "all":
#             use_ra_b9 += timet
#         else:
#             use_ra_b9[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#     if band == "ALMA_RB_10":
#         if ind == "all":
#             use_ra_b10 += timet
#         else:
#             use_ra_b10[ind] += timet
#         return use_ra_b3, use_ra_b4, use_ra_b6, use_ra_b7, use_ra_b8, use_ra_b9, use_ra_b10
#
#
#
# def conf_time(datedf):
#     tot_t = np.zeros(1440)
#     lst = np.arange(0, 24, 24. / (24 * 60.))
#     for i in datedf.iterrows():
#         lst_end = int(round(i[1].lst_end))
#
#         if i[1].available_time == 24:
#             t = np.ones(1440)
#
#         elif i[1].lst_start > i[1].lst_end:
#             t = np.zeros(1440)
#             t[lst > i[1].lst_start] += 1.
#             t[lst < i[1].lst_end] += 1.
#
#         else:
#             t = np.zeros(1440)
#             t[(lst > i[1].lst_start) & (lst < i[1].lst_end)] += 1.
#
#         tot_t += t
#
#     return tot_t
#
# c361b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-1"')
# c362b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-2"')
# c363b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-3"')
# c364b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-4"')
# c365b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-5"')
# c366b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-6"')
# c367b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-7"')
# c368b = summary.query('APRCGrade in ["A", "B"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-8"')
#
# tot_t1 = conf_time(date_df.query('C36_1 == 1'))
# tot_t2 = conf_time(date_df.query('C36_2 == 1'))
# tot_t3 = conf_time(date_df.query('C36_3 == 1'))
# tot_t4 = conf_time(date_df.query('C36_4 == 1'))
# tot_t5 = conf_time(date_df.query('C36_5 == 1'))
# tot_t6 = conf_time(date_df.query('C36_6 == 1'))
# tot_t7 = conf_time(date_df.query('C36_7 == 1'))
# tot_t8 = conf_time(date_df.query('C36_8 == 1'))
#
# ra1b, used1b = av_arrays(c361b)
# ra2b, used2b = av_arrays(c362b)
# ra3b, used3b = av_arrays(c363b)
# ra4b, used4b = av_arrays(c364b)
# ra5b, used5b = av_arrays(c365b)
# ra6b, used6b = av_arrays(c366b)
# ra7b, used7b = av_arrays(c367b)
# ra8b, used8b = av_arrays(c368b)
#
#
# def do_pre_plots(ra, used, tot_t, filename, title):
#     py.close()
#     py.figure(figsize=(11.69,8.27))
#     py.bar(ra, used[0] + used[1] + used[2] + used[3] + used[4] + used[5] + used[6], width=1.66666667e-02, ec='#4575b4', fc='#4575b4', label='Band 10')
#     py.bar(ra, used[0] + used[1] + used[2] + used[3] + used[4] + used[5], width=1.66666667e-02, ec='#91bfdb', fc='#91bfdb', label='Band 9')
#     py.bar(ra, used[0] + used[1] + used[2] + used[3] + used[4], width=1.66666667e-02, ec='#e0f3f8', fc='#e0f3f8', label='Band 8')
#     py.bar(ra, used[0] + used[1] + used[2] + used[3], width=1.66666667e-02, ec='#ffffbf', fc='#ffffbf', label='Band 7')
#     py.bar(ra, used[0] + used[1] + used[2], width=1.66666667e-02, ec='#fee090', fc='#fee090', label='Band 6')
#     py.bar(ra, used[0] + used[1], width=1.66666667e-02, ec='#fc8d59', fc='#fc8d59', label='Band 4')
#     py.bar(ra, used[0], width=1.66666667e-02, ec='#d73027', fc='#d73027', label='Band 3')
#     py.xlim(0,24)
#     py.xlabel('RA [hours]')
#     py.ylabel('Time Needed [hours]')
#     py.title(title)
#     py.plot(np.arange(0, 24, 24. / (24 * 60.)), tot_t, 'k--', label='100% efficiency')
#     py.plot(np.arange(0, 24, 24. / (24 * 60.)), tot_t * 0.6, 'k-.', label='60% efficiency')
#     py.legend(framealpha=0.7, fontsize='x-small')
#     py.savefig('/home/itoledo/Documents/' + filename, dpi=300)
#
# do_pre_plots(ra1b, used1b, tot_t1, 'C36-1.png', 'C36-1 Pressure')
# do_pre_plots(ra2b, used2b, tot_t2, 'C36-2.png', 'C36-2 Pressure')
# do_pre_plots(ra3b, used3b, tot_t3, 'C36-3.png', 'C36-3 Pressure')
# do_pre_plots(ra4b, used4b, tot_t4, 'C36-4.png', 'C36-4 Pressure')
# do_pre_plots(ra5b, used5b, tot_t5, 'C36-5.png', 'C36-5 Pressure')
# do_pre_plots(ra6b, used6b, tot_t6, 'C36-6.png', 'C36-6 Pressure')
# do_pre_plots(ra7b, used7b, tot_t7, 'C36-7.png', 'C36-7 Pressure')
# do_pre_plots(ra8b, used8b, tot_t8, 'C36-8.png', 'C36-8 Pressure')
#
# ra1b, used1b = av_arrays(c361b, minlst=-3., maxlst=3.)
# ra2b, used2b = av_arrays(c362b, minlst=-3., maxlst=3.)
# ra3b, used3b = av_arrays(c363b, minlst=-3., maxlst=3.)
# ra4b, used4b = av_arrays(c364b, minlst=-3., maxlst=3.)
# ra5b, used5b = av_arrays(c365b, minlst=-3., maxlst=3.)
# ra6b, used6b = av_arrays(c366b, minlst=-3., maxlst=3.)
# ra7b, used7b = av_arrays(c367b, minlst=-3., maxlst=3.)
# ra8b, used8b = av_arrays(c368b, minlst=-3., maxlst=3.)
#
# do_pre_plots(ra1b, used1b, tot_t1, 'C36-1_relax.png', 'C36-1 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra2b, used2b, tot_t2, 'C36-2_relax.png', 'C36-2 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra3b, used3b, tot_t3, 'C36-3_relax.png', 'C36-3 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra4b, used4b, tot_t4, 'C36-4_relax.png', 'C36-4 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra5b, used5b, tot_t5, 'C36-5_relax.png', 'C36-5 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra6b, used6b, tot_t6, 'C36-6_relax.png', 'C36-6 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra7b, used7b, tot_t7, 'C36-7_relax.png', 'C36-7 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots(ra8b, used8b, tot_t8, 'C36-8_relax.png', 'C36-8 Pressure (-3 to +3 HA, over 20 deg)')
#
#
#
# def conf_time_hours(datedf):
#     tot_t = np.zeros(24)
#     lst = np.arange(0, 24, 1)
#     for i in datedf.iterrows():
#         lst_end = int(round(i[1].lst_end))
#         if lst_end == 24:
#             lst_end = 23.99
#
#         if i[1].available_time == 24:
#             t = np.ones(24)
#
#         elif i[1].lst_start > i[1].lst_end:
#             t = np.zeros(24)
#             t[lst > i[1].lst_start] += 1.
#             t[lst < i[1].lst_end] += 1.
#
#         else:
#             t = np.zeros(24)
#             t[(lst > i[1].lst_start) & (lst < i[1].lst_end)] += 1.
#
#         tot_t += t
#
#     return tot_t
#
# tot_t1h = conf_time_hours(date_df.query('C36_1 == 1'))
# tot_t2h = conf_time_hours(date_df.query('C36_2 == 1'))
# tot_t3h = conf_time_hours(date_df.query('C36_3 == 1'))
# tot_t4h = conf_time_hours(date_df.query('C36_4 == 1'))
# tot_t5h = conf_time_hours(date_df.query('C36_5 == 1'))
# tot_t6h = conf_time_hours(date_df.query('C36_6 == 1'))
# tot_t7h = conf_time_hours(date_df.query('C36_7 == 1'))
# tot_t8h = conf_time_hours(date_df.query('C36_8 == 1'))
#
#
# def av_arrays_grade(sel, minlst=-2., maxlst=2.):
#
#     map_ra = np.arange(0, 24, 24. / (24 * 60.))
#     use_ra_high = np.zeros(1440)
#     use_ra_fill = np.zeros(1440)
#
#     for r in sel.iterrows():
#         data = r[1]
#
#         if data.RA == 0 or data.up == 24:
#             use_ra_high, use_ra_fill = add_grade('all', data.estimatedTime_SB / 24., data.APRCGrade,use_ra_high, use_ra_fill)
#             continue
#
#         if data.up > maxlst - minlst:
#             if maxlst < (data.RA / 15.) < 24 + minlst:
#                 rise = (data.RA / 15.) + minlst
#                 set = (data.RA / 15.) + maxlst
#                 up = maxlst - minlst
#                 use_ra_high, use_ra_fill = add_grade((map_ra > rise) & (map_ra < set), data.estimatedTime_SB / up, data.APRCGrade, use_ra_high, use_ra_fill)
#             else:
#                 if (data.RA / 15.) < maxlst:
#                     rise = 24 + minlst + (data.RA / 15.)
#                     set = (data.RA / 15.) + maxlst
#                     up = maxlst - minlst
#                 else:
#                     rise = (data.RA / 15.) + minlst
#                     set = maxlst - (24 - (data.RA / 15.))
#                     up = maxlst - minlst
#                 use_ra_high, use_ra_fill = add_grade(map_ra < set, data.estimatedTime_SB / up, data.APRCGrade, use_ra_high, use_ra_fill)
#                 use_ra_high, use_ra_fill = add_grade(map_ra > rise, data.estimatedTime_SB / up, data.APRCGrade, use_ra_high, use_ra_fill)
#             continue
#
#         if data.rise > data.set:
#             rise = data.rise
#             set = data.set
#             up = data.up
#             if data.up > maxlst - minlst:
#                 if set > maxlst:
#                     set = maxlst
#                 if rise < 24 + minlst:
#                     rise = 24 + minlst
#                 up = maxlst - minlst
#             use_ra_high, use_ra_fill = add_grade(map_ra < set, data.estimatedTime_SB / up, data.APRCGrade, use_ra_high, use_ra_fill)
#             use_ra_high, use_ra_fill = add_grade(map_ra > rise, data.estimatedTime_SB / up, data.APRCGrade, use_ra_high, use_ra_fill)
#         else:
#             rise = data.rise
#             set = data.set
#             up = data.up
#             if up > maxlst - minlst and data.ephem is False:
#                 if rise < data.RA / 15. + minlst:
#                     rise = data.RA / 15. + minlst
#                 if set > data.RA / 15. + maxlst:
#                     set = data.RA / 15. + maxlst
#                 up = maxlst - minlst
#                 use_ra_high, use_ra_fill = add_grade((map_ra > rise) & (map_ra < set), data.estimatedTime_SB / up, data.APRCGrade, use_ra_high, use_ra_fill)
#
#     return map_ra, [use_ra_high, use_ra_fill]
#
#
# def add_grade(ind, timet, grade, use_ra_high, use_ra_fill):
#
#     if grade in ["A", "B"]:
#         if ind == "all":
#             use_ra_high += timet
#         else:
#             use_ra_high[ind] += timet
#         return use_ra_high, use_ra_fill
#     if grade == "C":
#         if ind == "all":
#             use_ra_fill += timet
#         else:
#             use_ra_fill[ind] += timet
#         return use_ra_high, use_ra_fill
#
# newc = pd.read_excel('/home/itoledo/Documents/C36-1-6_data-final.xls')
# ccodes = newc[newc['APRC Grade'] == "C"]['Prj. Code'].unique()
# csgnames = newc[newc['APRC Grade'] == "C"]['SG Name [SB]'].unique()
# summary['APRCGrade'] = summary.apply(lambda x: x['APRCGrade'] if x['APRCGrade'] in ["A", "B"] else "U", axis=1)
# summary['APRCGrade'] = summary.apply(lambda x: "C" if x['CODE'] in ccodes else x['APRCGrade'], axis=1)
# summary['SG_sel'] = summary.apply(lambda x: True if ((x['APRCGrade'] in ["A", "B"]) or ((x['CODE'] in ccodes) and (x['sg_name'] in csgnames))) else False, axis=1)
#
#
# c361bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-1" and SG_sel == True')
# c362bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-2" and SG_sel == True')
# c363bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-3" and SG_sel == True')
# c364bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-4" and SG_sel == True')
# c365bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-5" and SG_sel == True')
# c366bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-6" and SG_sel == True')
# c367bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-7" and SG_sel == True')
# c368bf = summary.query('APRCGrade in ["A", "B", "C"] and twelve_good > 0 and array == "TWELVE-M" and BestConf == "C36-8" and SG_sel == True')
#
# ra1bf, used1bf = av_arrays_grade(c361bf)
# ra2bf, used2bf = av_arrays_grade(c362bf)
# ra3bf, used3bf = av_arrays_grade(c363bf)
# ra4bf, used4bf = av_arrays_grade(c364bf)
# ra5bf, used5bf = av_arrays_grade(c365bf)
# ra6bf, used6bf = av_arrays_grade(c366bf)
# ra7bf, used7bf = av_arrays_grade(c367bf)
# ra8bf, used8bf = av_arrays_grade(c368bf)
#
#
# def do_pre_plots_fill(ra, used, tot_t, filename, title):
#     py.close()
#     py.figure(figsize=(11.69,8.27))
#     py.bar(ra, used[0] + used[1], width=1.66666667e-02, ec='#e41a1c', fc='#e41a1c', label='Fillers')
#     py.bar(ra, used[0], width=1.66666667e-02, ec='#4daf4a', fc='#4daf4a', label='High Priority')
#     py.xlim(0,24)
#     py.xlabel('RA [hours]')
#     py.ylabel('Time Needed [hours]')
#     py.title(title)
#     py.plot(np.arange(0, 24, 24. / (24 * 60.)), tot_t, 'k--', label='100% efficiency')
#     py.plot(np.arange(0, 24, 24. / (24 * 60.)), tot_t * 0.6, 'k-.', label='60% efficiency')
#     py.legend(framealpha=0.7, fontsize='x-small')
#     py.savefig('/home/itoledo/Documents/' + filename, dpi=300)
#
#
# do_pre_plots_fill(ra1bf, used1bf, tot_t1, 'C36-1_grade.png', 'C36-1 Pressure')
# do_pre_plots_fill(ra2bf, used2bf, tot_t2, 'C36-2_grade.png', 'C36-2 Pressure')
# do_pre_plots_fill(ra3bf, used3bf, tot_t3, 'C36-3_grade.png', 'C36-3 Pressure')
# do_pre_plots_fill(ra4bf, used4bf, tot_t4, 'C36-4_grade.png', 'C36-4 Pressure')
# do_pre_plots_fill(ra5bf, used5bf, tot_t5, 'C36-5_grade.png', 'C36-5 Pressure')
# do_pre_plots_fill(ra6bf, used6bf, tot_t6, 'C36-6_grade.png', 'C36-6 Pressure')
# do_pre_plots_fill(ra7bf, used7bf, tot_t7, 'C36-7_grade.png', 'C36-7 Pressure')
# do_pre_plots_fill(ra8bf, used8bf, tot_t8, 'C36-8_grade.png', 'C36-8 Pressure')
#
# ra1bf, used1bf = av_arrays_grade(c361bf, minlst=-3., maxlst=3.)
# ra2bf, used2bf = av_arrays_grade(c362bf, minlst=-3., maxlst=3.)
# ra3bf, used3bf = av_arrays_grade(c363bf, minlst=-3., maxlst=3.)
# ra4bf, used4bf = av_arrays_grade(c364bf, minlst=-3., maxlst=3.)
# ra5bf, used5bf = av_arrays_grade(c365bf, minlst=-3., maxlst=3.)
# ra6bf, used6bf = av_arrays_grade(c366bf, minlst=-3., maxlst=3.)
# ra7bf, used7bf = av_arrays_grade(c367bf, minlst=-3., maxlst=3.)
# ra8bf, used8bf = av_arrays_grade(c368bf, minlst=-3., maxlst=3.)
#
# do_pre_plots_fill(ra1bf, used1bf, tot_t1, 'C36-1_grade_relax.png', 'C36-1 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra2bf, used2bf, tot_t2, 'C36-2_grade_relax.png', 'C36-2 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra3bf, used3bf, tot_t3, 'C36-3_grade_relax.png', 'C36-3 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra4bf, used4bf, tot_t4, 'C36-4_grade_relax.png', 'C36-4 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra5bf, used5bf, tot_t5, 'C36-5_grade_relax.png', 'C36-5 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra6bf, used6bf, tot_t6, 'C36-6_grade_relax.png', 'C36-6 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra7bf, used7bf, tot_t7, 'C36-7_grade_relax.png', 'C36-7 Pressure (-3 to +3 HA, over 20 deg)')
# do_pre_plots_fill(ra8bf, used8bf, tot_t8, 'C36-8_grade_relax.png', 'C36-8 Pressure (-3 to +3 HA, over 20 deg)')

