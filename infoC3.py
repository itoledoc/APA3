import DataBase3 as db
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

spectral_agg = pd.merge(spectral_agg, spectral_agg2, left_index=True, right_index=True)

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

has_cont_su = sb_fs_spw[(sb_fs_spw.lineName.str.contains('cont') == True) | (sb_fs_spw.lineName.str.contains('Cont') == True)].SB_UID.unique()
schedblocks['has_cont_only'] = schedblocks.apply(lambda x: True if x['SB_UID'] in has_cont_su else False, axis=1)

has_narrow_spw = sb_fs_spw.groupby('SB_UID').EffectiveBandwidth.agg([pd.np.min, pd.np.max]).reset_index().query('amin < 1').SB_UID.unique()
schedblocks['has_narrow_spw'] = schedblocks.apply(lambda x: True if x['SB_UID'] in has_narrow_spw else False, axis=1)

has_only_narrow_spw = sb_fs_spw.groupby('SB_UID').EffectiveBandwidth.agg([pd.np.min, pd.np.max]).reset_index().query('amax < 1').SB_UID.unique()
schedblocks['has_only_narrow_spw'] = schedblocks.apply(lambda x: True if x['SB_UID'] in has_only_narrow_spw else False, axis=1)




merge4 = pd.merge(merge3, schedblocks, left_on=['OBSPROJECT_UID', 'sg_name'], right_on=['OBSPROJECT_UID', 'SG_ID'], suffixes=['_SG', '_SB'])

merge4.to_csv('/home/itoledo/Documents/prj_sg_sb.csv')




datas.sg_targets.to_csv('/home/itoledo/Documents/sg_targets.csv')
datas.target.to_csv('/home/itoledo/Documents/sb_target.csv')
datas.orederedtar.to_csv('/home/itoledo/Documents/sb_order_target.csv')
datas.scienceparam.to_csv('/home/itoledo/Documents/sb_sciparam.csv')
datas.baseband.to_csv('/home/itoledo/Documents/sb_basebands.csv')
datas.spectralconf.to_csv('/home/itoledo/Documents/sb_specconf.csv')
datas.spectralwindow.to_csv('/home/itoledo/Documents/sb_specwindow.csv')
datas.fieldsource.to_csv('/home/itoledo/Documents/sb_fieldsource.csv')

# sb_scitar_num = sb_fs_spw.groupby('SB_UID').fieldRef.nunique().reset_index()
#
# total = 0
#
# for i in datas.schedblocks.iterrows():
#     ouid = i[1]['OBSPROJECT_UID']
#     sgn = i[1]['SG_ID']
#     c = len(datas.schedblocks.query('OBSPROJECT_UID == @ouid and SG_ID == @sgn and array == "TWELVE-M"'))
#     if c > 1:
#     	total += 1
