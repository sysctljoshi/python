#!/usr/bin/python

import psycopg2
import sys
import os
import csv
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
import base64
import datetime
import time
import os
import subprocess
from datetime import datetime, timedelta
from smtplib import SMTP
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
i = datetime.today()
today=(time.strftime("%Y-%m-%d"))
tod=(time.strftime("%Y%m%d"))
toda1="%sT012201" %tod

def main():
 os.system("ssh postgres@backup_db /usr/local/pgsql/bin/pg_ctl stop -D /srv/database/data")
 os.system("ssh root@backup_db rm -rf /srv/database/data/*")
 os.system("barman recover --target-time \"%s 09:30\"  --remote-ssh-command \"ssh root@backup_db\" db1 %s /srv/database/data/" %(today, toda1) )
 os.system("ssh root@backup_db cp /srv/postgresql.conf /srv/database/data/")
 os.system("ssh root@backup_db cp /srv/pg_hba* /srv/canvera/pg_9.2/")
 os.system("ssh root@backup_db chown -R postgres /srv/canvera/pg_9.2/")
 os.system("ssh -f root@backup_db \'su -s /bin/bash -c \"/usr/local/pgsql/bin/pg_ctl start -D /srv/database/data\" postgres \' ")
 time.sleep(60)
 conn_string = "host='backup_db' user='readonly' dbname='db1' password='readonly' port=5432" 
 conn = psycopg2.connect(conn_string)
 conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
 cursor = conn.cursor()
 cursor.execute("""select count(*) from db1.subbatchaudit where changedate <= \'%s 03:00:00\'""" %today)
 results = cursor.fetchall()
 for r in results:
    barman_val=(r[0])
 cursor.close()
 conn.close()

 conn_string1 = "host='prod_db' user='readonly' dbname='canvera' password='readonly' port=5432"
 conn1 = psycopg2.connect(conn_string1)
 conn1.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
 cursor1 = conn1.cursor()
 cursor1.execute("""select count(*) from db1.subbatchaudit where changedate <= \'%s 03:00:00\'""" %today)
 results1 = cursor1.fetchall()
 for r1 in results1:
    prod_val=(r1[0])
 cursor1.close()
 conn1.close()
 
 if ( prod_val == barman_val ) :
   message = "Select Query returned same value for both DB - prod and backup - Values returned are - prod is %s and backup is %s" %(prod_val,barman_val)
   print message
   from email.MIMEMultipart import MIMEMultipart
   from email.MIMEText import MIMEText
   from email.MIMEBase import MIMEBase
   from email import encoders
   fromaddr = "XXX"
   ccaddr = ['XXXX']
   toaddr = ['XXXX']
   msg = MIMEMultipart()
   msg['From'] = fromaddr
   msg['To'] = ', '.join(toaddr)
   msg['Cc'] = ', '.join(ccaddr)
   msg['Subject'] = "Barman - No issues found while restoring Backup DB !! All Good !!"
   body = "%s" %message
   msg.attach(MIMEText(body, 'plain'))
   part = MIMEBase('application', 'octet-stream')
   encoders.encode_base64(part)
   server = smtplib.SMTP('x.x.x.x')
   text = msg.as_string()
   server.sendmail(fromaddr, toaddr+ccaddr, text)
   server.quit()
 else : 
  message = "Select Query returned different value for both DB - prod and backup - Values returned are - prod is %s and backup is %s" %(prod_val,barman_val)  
 
  from email.MIMEMultipart import MIMEMultipart
  from email.MIMEText import MIMEText
  from email.MIMEBase import MIMEBase
  from email import encoders
  fromaddr = "XXXX"
  ccaddr = ['XXXX']
  toaddr = ['XXXX']
  msg = MIMEMultipart()
  msg['From'] = fromaddr
  msg['To'] = ', '.join(toaddr)
  msg['Cc'] = ', '.join(ccaddr)
  msg['Subject'] = "Barman - Issues found while restoring DB1 DB"
  body = "%s" %message
  msg.attach(MIMEText(body, 'plain'))
  part = MIMEBase('application', 'octet-stream')
  encoders.encode_base64(part)
  server = smtplib.SMTP('x.x.x.x')
  text = msg.as_string()
  server.sendmail(fromaddr, toaddr+ccaddr, text)
  server.quit()

if __name__ == "__main__":
          main()

