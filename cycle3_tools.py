__author__ = 'itoledo'

import pandas as pd
import datetime as dt
import ephem
import numpy as np

alma1 = ephem.Observer()
alma1.lat = '-23.0262015'
alma1.long = '-67.7551257'
alma1.elev = 5060
alma1.horizon = ephem.degrees(str('20'))


es_cycle3 = [
    ['2015-10-13', '2015-10-26', 'block21', [8]],
    ['2015-11-10', '2015-11-30', 'block21', [7]],
    ['2015-12-22', '2016-01-18', 'block21', [1]],
    ['2016-01-26', '2016-01-30', 'block21', [1]],
    ['2016-03-01', '2016-04-04', 'block21', [2]],
    ['2016-04-12', '2016-05-09', 'block21', [3]],
    ['2016-05-17', '2016-06-13', 'block21', [4]],
    ['2016-06-21', '2016-07-11', 'block21', [5]],
    ['2016-07-19', '2016-08-29', 'block21', [6]],
    ['2016-09-13', '2016-09-26', 'block21', [7]],
]

ar_res = [3.4, 1.8, 1.2, 0.7, 0.5, 0.3, 0.1, 0.075]


def create_dates(data_arin):
    data_ar = []

    for e in data_arin:

        startd = dt.datetime.strptime(e[0], '%Y-%m-%d')
        endd = dt.datetime.strptime(e[1], '%Y-%m-%d')
        confs = [0, 0, 0, 0, 0, 0, 0, 0]
        for c in e[3]:
            confs[c - 1] = 1
        while startd <= endd:
            if startd.isoweekday() not in [4, 5, 6, 7]:
                dr = [
                    startd + dt.timedelta(hours=20),
                    startd + dt.timedelta(hours=20 + 16),
                    e[2]]
                dr.extend(confs)
                data_ar.append(dr)
            else:
                dr = [
                    startd + dt.timedelta(hours=20),
                    startd + dt.timedelta(hours=20 + 24),
                    e[2]]
                dr.extend(confs)
                data_ar.append(dr)
            startd += dt.timedelta(1)

    return data_ar


def observable(ra1, dec1, alma, isephem, minAR, maxAR, array, sbuid):

    datet = alma.date
    conf = [None, None, None, None, None, None, None, None]
    twelve_good = 0

    if array == "TWELVE-M":
        for i, a in enumerate(ar_res):
            if minAR <= a <= maxAR:
                conf[i] = 'C36-' + str(i + 1)
                twelve_good += 1

    if isephem:
        return pd.Series([sbuid, 0., 23.99999, 24., 'ephem', conf[0], conf[1],
                          conf[2], conf[3], conf[4], conf[5], conf[6], conf[7],
                          twelve_good],
                         index=['SB_UID', 'rise', 'set', 'up', 'note', 'C36_1',
                                'C36_2', 'C36_3', 'C36_4', 'C36_5', 'C36_6',
                                'C36_7', 'C36_8', 'twelve_good'])

    obj = ephem.FixedBody()
    obj._ra = pd.np.deg2rad(ra1)
    obj._dec = pd.np.deg2rad(dec1)
    obj.compute(alma)
    if obj.circumpolar:
        return pd.Series([sbuid,  0., 23.99999, 24., 'circumpol', conf[0],
                          conf[1], conf[2], conf[3], conf[4], conf[5], conf[6],
                          conf[7], twelve_good],
                         index=['SB_UID', 'rise', 'set', 'up', 'note', 'C36_1',
                                'C36_2', 'C36_3', 'C36_4', 'C36_5', 'C36_6',
                                'C36_7', 'C36_8', 'twelve_good'])

    sets = alma.next_setting(obj)
    rise = alma.previous_rising(obj)
    alma.date = rise
    lstr = alma.sidereal_time()
    alma.date = sets
    lsts = alma.sidereal_time()
    alma.date = datet
    lstr = np.rad2deg(lstr) / 15.
    lsts = np.rad2deg(lsts) / 15. - 2.
    if lsts < 0:
        lsts += 24
    if lstr > lsts:
        up = 24. - lstr + lsts
    else:
        up = lsts - lstr
    return pd.Series([sbuid, lstr, lsts, up, 'OK', conf[0], conf[1], conf[2],
                      conf[3], conf[4], conf[5], conf[6], conf[7], twelve_good],
                     index=['SB_UID', 'rise', 'set', 'up', 'note', 'C36_1',
                            'C36_2', 'C36_3', 'C36_4', 'C36_5', 'C36_6',
                            'C36_7', 'C36_8', 'twelve_good'])


def day_night(sdate, edate, alma):

    datet = alma.date
    alma.horizon = ephem.degrees(str('-20'))
    obj = ephem.Sun()

    alma.date = sdate
    lst_start = alma.sidereal_time()
    sun_set = alma.next_setting(obj)
    alma.date = sun_set
    alma.horizon = ephem.degrees(str('0'))
    lst_dark = alma.sidereal_time()

    alma.horizon = ephem.degrees(str('-20'))
    alma.date = edate
    lst_end = alma.sidereal_time()
    sun_rise = alma.previous_rising(obj)
    alma.date = sun_rise
    alma.horizon = ephem.degrees(str('0'))
    lst_dawn = alma.sidereal_time()

    alma.date = datet

    lst_start = np.rad2deg(lst_start) / 15.
    lst_dark = np.rad2deg(lst_dark) / 15.
    lst_end = np.rad2deg(lst_end) / 15.
    lst_dawn = np.rad2deg(lst_dawn) / 15.

    return pd.Series([lst_start, lst_dark, lst_end, lst_dawn],
                     index=['lst_start', 'lst_dusk', 'lst_end', 'lst_dawn'])


def avail_calc(orise, oset, conf1, conf2, conf3, conf4, conf5, conf6, conf7,
               conf8, up, band, datedf):

    # First, is observable?
    confnames_df = ['C36_1', 'C36_2', 'C36_3', 'C36_4', 'C36_5', 'C36_6',
                    'C36_7', 'C36_8']
    cf = np.zeros(8)
    for i, a in enumerate([conf1, conf2, conf3, conf4, conf5, conf6, conf7,
                           conf8]):
        if a:
            cf[1] = 1
    hup = 0.
    safe = 0
    crit = 0
    wend = 0
    based = dt.datetime(2015, 1, 1)
    if orise < oset:
        b1 = based + dt.timedelta(hours=orise)
        b2 = based + dt.timedelta(hours=oset)
        b3 = based + dt.timedelta(days=1, hours=orise)
        b4 = based + dt.timedelta(days=1, hours=oset)
    else:
        b1 = based
        b2 = based + dt.timedelta(hours=oset)
        b3 = based + dt.timedelta(hours=orise)
        b4 = based + dt.timedelta(days=1, hours=oset)

    for di in datedf.index:
        l = datedf.ix[di]
        confs = l[confnames_df].values

        if 1 not in cf * confs:
            continue
        else:
            if ((l.start < dt.datetime(2016, 4, 1)) and
                    (l.start > dt.datetime(2016, 1, 1)) and
                    band in ['ALMA_RB_08', 'ALMA_RB_09']):
                continue

        if band in ['ALMA_RB_08', 'ALMA_RB_09']:
            dstart = l.lst_dusk
            dend = l.lst_dawn
            check_not24 = True
        elif band == 'ALMA_RB_07':
            dstart = l.lst_dusk
            dend = l.lst_dawn + 3.
            check_not24 = False
            if dend > 24:
                dend -= 24
                check_not24 = True

        else:
            dstart = l.lst_start
            dend = l.lst_end
            check_not24 = False
            if l.start.weekday() not in [4, 5]:
                dend = l.lst_end - 1.5
                if dend < 0:
                    dend += 24
                check_not24 = True

        if dstart < dend and check_not24:
            dstart = based + dt.timedelta(hours=dstart)
            dend = based + dt.timedelta(hours=dend)
        else:
            dstart = based + dt.timedelta(hours=dstart)
            dend = based + dt.timedelta(days=1, hours=dend)

        critt = 0
        wendt = 0
        safet = 0

        for r in [[b1, b2], [b3, b4]]:

            if (r[1] < dstart) or (r[0] > dend):
                hup += 0
            elif (r[0] < dstart) and (r[1] > dend):
                hup += (dend - dstart).total_seconds() / 3600.
                safet += 1
            elif (r[0] > dstart) and (r[1] < dend):
                hup += (r[1] - r[0]).total_seconds() / 3600.
                safet += 1
            else:
                tl = [r[1], r[0], dstart, dend]
                tl.sort()
                # print tl
                delta = ((tl[2] - tl[1]).total_seconds() / 3600.)
                if 0 < delta < 2.:
                    hup += delta
                    critt += 1
                else:
                    hup += delta
                    safe += 1

            if ((critt > 0) or (safet > 0)) and (l.start.weekday() in [4, 5]):
                wendt += 1
        if critt > 0:
            crit += 1
        if wendt > 0:
            wend += 1
        if safet > 0:
            safe += 1

    return pd.Series([hup, safe, crit, wend],
                     index=['available_hours', 'days', 'days_crit', 'weekend'])


def sim(lst, limit_bands, sb):

    conv_f = 24 / 23.9344699

    ha = lst - sb['RA']
    if ha > 12:
        ha = 24 - ha
    elif ha < -12:
        ha += 24

    clo = abs(-1 - ha)

    if sb['band'] in limit_bands:
        obs = False
        return pd.Series([sb['SB_UID'], obs, ha, clo],
                         index=['SB_UID', 'obs', 'HA', 'clo'])

    obs = False
    if sb['rise'] < sb['set']:
        if sb['rise'] < lst < (sb['set'] - sb['SB_ETC2_exec'] * 1.1 * conv_f):
            obs = True
    else:
        if sb['rise'] < lst < 24:
            obs = True
        elif lst < sb['set']:
            obs = True

    if (ha <= -4) or (ha >= 4):
        obs = False

    return pd.Series([sb['SB_UID'], obs, ha, clo],
                     index=['SB_UID', 'obs', 'HA', 'clo'])


def sel(df, lst, limitbands, array, out):

    df1 = df.copy()
    selec = ''
    for a in array:
        if len(selec) == 0:
            selec = a + ' == 1'
        else:
            selec = selec + ' or ' + a + ' == 1'

    df1 = df1.query(selec)

    df1 = df1.query('SBremExec > 0')

    df2 = df1.query('observed > 0 and PRJ_LETTER_GRADE in ["A", "B"]')

    r = df2.apply(
        lambda row: sim(lst, limitbands, row), axis=1)
    try:
        SB_UID = r.query('obs == True').sort('clo').SB_UID.values[0]
    except IndexError:
        SB_UID = None
    except Exception, e:
        print e
        SB_UID = None

    if SB_UID is None:

        df2 = df1.query('PRJ_LETTER_GRADE in ["A", "B"]')
        r = df2.apply(lambda row: sim(lst, limitbands, row), axis=1)
        try:
            SB_UID = r.query('obs == True').sort('clo').SB_UID.values[0]
        except IndexError:
            SB_UID = None

    if SB_UID is None:

        r = df1.apply(lambda row: sim(lst, limitbands, row), axis=1)
        try:
            SB_UID = r.query('obs == True').sort('clo').SB_UID.values[0]
        except IndexError:
            SB_UID = None

    if SB_UID:
        df.loc[SB_UID, 'SBremExec'] -= 1

    out.append(SB_UID)

    return out, SB_UID


def runsim(date_df, df, alma):

    dft = df.copy()
    out = []
    out1 = []
    obj = ephem.Sun()
    for r in date_df.index:
        ti = date_df.loc[r, 'start'] + dt.timedelta(hours=1)
        end = date_df.loc[r, 'end'] - dt.timedelta(hours=1)
        array = []
        for a in ['C34_1', 'C34_2', 'C34_3', 'C34_4', 'C34_5', 'C34_6',
                  'C34_7']:
            if date_df.loc[r, a] == 1:
                array.append(a)
        print date_df.loc[r, 'block']
        while ti < end:
            alma.date = ti
            obj.compute(alma)
            day = True
            if obj.alt <= ephem.degrees('-20'):
                day = False

            if ti <= dt.datetime(2015, 5, 6):
                limit_b = ['ALMA_RB_08', 'ALMA_RB_09']
            else:
                limit_b = []
            if day:
                limit_b = (['ALMA_RB_07', 'ALMA_RB_08', 'ALMA_RB_09'])

            lst = np.rad2deg(alma.sidereal_time()) / 15.
            out, sb = sel(dft, lst, limit_b, array, out)
            if sb:
                dur = dft.loc[sb, 'SB_ETC2_exec']
            try:
                sbr = int(dft.loc[sb, 'SBremExec'])
                gr = dft.loc[sb, 'PRJ_LETTER_GRADE']
                ra = dft.loc[sb, 'RA']
                out1.append(
                    [ti, lst, day, len(limit_b), array, sb, sbr, ra, gr, dur])
            except ValueError:
                out1.append(
                    [ti, lst, day, len(limit_b), array, sb, None, None, None, 0])

            if sb:
                dur = dft.loc[sb, 'SB_ETC2_exec'] * 1.1
                ti += dt.timedelta(hours=dur)
            else:
                ti += dt.timedelta(minutes=10)

    return dft, out1