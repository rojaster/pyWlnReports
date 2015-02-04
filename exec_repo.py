#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import mktime
from datetime import datetime
import sys
import time
import pymysql


from wialon import Wialon
from wialon import WialonError
from wialon import flags


assert WialonError
assert Wialon
assert flags
assert pymysql


# globals, cause i'm lazy angry monkey man
sess_state = 1
sess_timeout = 10
wln_handle = None
cl_tmz_offset_req = -time.timezone - 18000 # +05:00GMT
cl_tmz_offset_resp = time.timezone + 18000


def login(user='login', passwd='pass'):
    """
    Core Rest_api login : core/login

    :type user: str
    :type passwd: str
    :param user:
    :param passwd:
    :rtype : object
    """
    return wln_handle.core_login(user=user, password=passwd)




def logout():
    """
    Core Session Logout: core/logout
    Handle successful logout only

    :rtype : None
    """
    try:
        wln_handle.core_logout()
    except WialonError as we:
        if 0 == we.code:
            print('logout successful')
        else:
            raise WialonError(we.code, 'logout assertion')



# TODO : method should be invoked as subprocess and leave it when main loop is over
def session_standing():
    """
    Makes standalone session

    :rtype : dict
    """
    global sess_state
    global sess_timeout

    while sess_state:
        print(wln_handle.avl_evts())
        time.sleep(sess_timeout)


def db_records_saving(records_list = None):
    """

    May be this shit does not working, but who knows

    :rtype : None
    """
    if records_list is None:
        print('Where is your records??')
        return

    i_sql = """
                INSERT INTO `tblreport`(
                                        `id`, `place_work`, `date`, `object_name`, `object_type`, `object_wln_id`,
                                        `start_time_int`, `end_time_int`, `motohours`, `energy_p`, `ph_sred`, `nagruzka`,
                                        `energy_q`, `fuel_density`, `fuel_used`, `fuel_rate`, `fuel_in`, `fuel_start_period`,
                                        `fuel_end_period`, `p_start`, `p_end`, `q_start`, `q_end`, `s_start`, `s_end`, `cos_fi`,
                                        `local_energy`, `trans_param`
                                        )
                            VALUES (
                                    NULL,%(place_work)s,%(date)s,%(object_name)s,%(object_type)s,%(object_wln_id)s,FROM_UNIXTIME(%(start_time_int)s),FROM_UNIXTIME(%(end_time_int)s),
                                    %(motohours)s,%(energy_p)s,%(ph_sred)s,%(nagruzka)s,%(energy_q)s,%(fuel_density)s,%(fuel_used)s,%(fuel_rate)s,
                                    %(fuel_in)s,%(fuel_start_period)s,%(fuel_end_period)s,%(p_start)s,%(p_end)s,%(q_start)s,%(q_end)s,%(s_start)s,
                                    %(s_end)s,%(cos_fi)s,%(local_energy)s,%(trans_param)s
                                    )
"""

    print('------ Connect to database')
    conn = pymysql.connect( host = 'localhost' , user = 'uhelper', password = 'zxcdewqasd', db = '_helper', charset = 'utf8' )
    cur = conn.cursor()

    print('------ Insert new records into table')
    for rec in records_list:
        cur.execute(i_sql,rec)

    print('------ Committing changes at database')
    conn.commit()

    print('------ Close connection to database and leave')
    cur.close()
    conn.close()



def main():
    """
        Main Execution Loop
    """

    global wln_handle
    wln_handle = Wialon()

    print('-- Login and set SID --')
    res = login()
    wln_handle.sid = res['eid']
    wln_handle.avl_evts()

    print('-- Set session flags --')
    print('------ Set avl_resource flag ')
    # reports resource
    avl_resource = wln_handle.core_update_data_flags(spec=[{ 'type'  : 'type',
                                                             'data'  : 'avl_resource',
                                                             'mode'  : 1,
                                                             'flags' : flags.ITEM_RESOURCE_DATAFLAG_REPORTS }])

    print('------ Set avl_unit_group ')
    # unit group info
    avl_u_groups = wln_handle.core_update_data_flags(spec=[{ 'type'  : 'type',
                                                             'data'  : 'avl_unit_group',
                                                             'mode'  : 1,
                                                             'flags' : flags.ITEM_RESOURCE_DATAFLAG_REPORTS | flags.ITEM_DATAFLAG_BASE }])

    print('------ Set avl_unit ')
    # unit info
    avl_unit_res = wln_handle.core_update_data_flags(spec=[{ 'type'  : 'type',
                                                             'data'  : 'avl_unit',
                                                             'mode'  : 1,
                                                             'flags' : flags.ITEM_RESOURCE_DATAFLAG_REPORTS | flags.ITEM_DATAFLAG_BASE }])


    # TODO : need set date interval by days
    #st_range = 1421002800
    #fin_range = 1421089140
    report_running_date = datetime.strptime(sys.argv[1], "%d/%m/%y %H:%M")
    st_range = int( mktime( report_running_date.timetuple() ) + cl_tmz_offset_req )
    fin_range = int( mktime( datetime.strptime(sys.argv[2], "%d/%m/%y %H:%M").timetuple() ) + cl_tmz_offset_req )


    print('-- Report run : ' + sys.argv[1] + " : " + sys.argv[2])
    rep_exec = wln_handle.report_exec_report(reportResourceId  = 12122660,
                                             reportTemplateId  = 1,
                                             reportObjectId    = 12244464,
                                             reportObjectSecId = 0,
                                             interval = { 'from' : st_range, 'to' : fin_range, 'flags' : 0 })

    report_table_list = list()
    i = 0
    for tab in rep_exec['reportResult']['tables']:
        report_table_list.append( dict( tabIndex=i, lbl = tab['label'], rows = tab['rows'] ) )
        i = i + 1

    report_table_data = list()
    for t_rec in report_table_list:
        res = wln_handle.report_get_result_rows( tableIndex = t_rec['tabIndex'], indexFrom = 0, indexTo = t_rec['rows'] )
        report_table_data.append( dict( tabIndex = t_rec['tabIndex'], data = res ) )

    # get 'moto_rashod_virobotka'
    for i in report_table_data:
        if 0 == i['tabIndex']:
            refills = i['data']

        if 1 == i['tabIndex']:
            moto_rate = i['data']

        if 2 == i['tabIndex']:
            remain_fuel = i['data']

        if 3 == i['tabIndex']:
            q_table = i['data']


    # list of units of unit group with description and id, fuel_in, fuel_start_per and fuel_end_per
    u_of_group = dict()
    for u in avl_u_groups[1]['d']['u']:

        d = ''
        f_in = ''
        r_fl_st = ''
        r_fl_fn = ''
        q_start = ''
        q_end = ''

        # find WLN_ID, WLN_UNIT_NAME by unit id's list from unit_group
        for i in avl_unit_res:
            if u == i['d']['id']:
                d = i['d']['nm']

                # find FUEL_IN param by unit NAME in Zapravki table resul
                for k in refills:
                    if d == k['c'][1]:
                        f_in = k['c'][3].replace(' lt', '')
                        break

                # find FUEL_START_PREIOD and FUELD_END_PERIOD by unit NAME
                for x in remain_fuel:
                    if d == x['c'][1]:
                        r_fl_st = x['c'][3].replace(' lt','')
                        r_fl_fn = x['c'][4].replace(' lt','')
                        break

                # find Q_START and Q_END
                for z in q_table:
                    if d == z['c'][1]:
                        q_start = z['c'][3]
                        q_end = z['c'][4]

                break

        u_of_group[d] = [str(u),f_in,[r_fl_st, r_fl_fn],[q_start,q_end]]


    # create a result records list to save in database
    db_table = list()
    for rec in moto_rate:
        if rec['c'][2] == '':
            rec['c'][2] = report_running_date.strftime( '%Y-%m-%d' )
        db_rec = dict(
                           place_work = avl_u_groups[1]['d']['nm'],
                                 date = rec['c'][2],
                          object_name = rec['c'][1],
                          object_type = '', # non fill
                        object_wln_id = u_of_group.get(rec['c'][1])[0],
                       start_time_int = str(int(rec['t1']) + cl_tmz_offset_resp),
                         end_time_int = str(int(rec['t2']) + cl_tmz_offset_resp),
                            motohours = rec['c'][5],
                             energy_p = rec['c'][6].replace(' km',''),
                              ph_sred = '', # non fill
                             nagruzka = '', # non fill
                             energy_q = '', # non fill
                         fuel_density = '',
                            fuel_used = rec['c'][7].replace(' lt',''),
                            fuel_rate = rec['c'][8].replace(' lt/100 km',''),
                              fuel_in = u_of_group.get(rec['c'][1])[1],
                    fuel_start_period = u_of_group.get(rec['c'][1])[2][0],
                      fuel_end_period = u_of_group.get(rec['c'][1])[2][1],
                              p_start = rec['c'][9],
                                p_end = rec['c'][10],
                              q_start = u_of_group.get(rec['c'][1])[3][0],
                                q_end = u_of_group.get(rec['c'][1])[3][1],
                              s_start = '', # non fill
                                s_end = '', # non fill
                               cos_fi = rec['c'][11],
                         local_energy = '', # non fill
                          trans_param = ''
                    ) # db_rec
        db_table.append( db_rec )


    print('-- Saving results into database')
    # save into database
    db_records_saving( db_table )

    # end report session
    logout()




if __name__ == "__main__":
    try:
        main()
    except WialonError as we:
        print(we)
    except pymysql.Error as sql_e:
        print(sql_e)
    except Exception as e:
        print(e)
