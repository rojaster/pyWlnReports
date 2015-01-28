#!/usr/bin/env python
# -*- coding: utf-8 -*-


from time import sleep
from datetime import datetime, timedelta
import os
import sys

formatter = "%d/%m/%y %H:%M"

st_date = sys.argv[1] # "01/10/14 17:00"
fn_date = sys.argv[2] # "02/10/14 16:59"

c_date = datetime.strptime( st_date, formatter )
stopper_date = datetime.strptime( sys.argv[3], formatter ) # stopper_date = datetime.strptime( "23/01/15 16:59", formatter )

delta = timedelta( days=1 )

#day_sec = 86400

while( c_date < stopper_date ):
    print( "start : {0} , finish : {1}".format(st_date, fn_date) )

    os.system('C:/Python27/python.exe H:/D_backup/My_works/Olega/pyWlnReports/exec_repo.py "{0}" "{1}"'.format(st_date, fn_date) )

    print('\n')
    print('#'*25)

    c_date  =  datetime.strptime( st_date, formatter ) + delta
    st_date = c_date.strftime( formatter )
    fn_date = ( datetime.strptime( fn_date, formatter ) + delta ).strftime( formatter )

    # if need seconds
    # st_date = datetime.fromtimestamp( int( mktime( datetime.strptime( st_date, formatter ).timetuple() ) ) + day_sec ).strftime(formatter)
    # fn_date = datetime.fromtimestamp( int( mktime ( datetime.strptime( fn_date, formatter ).timetuple() ) ) + day_sec ).strftime(formatter)

    sleep(3)
