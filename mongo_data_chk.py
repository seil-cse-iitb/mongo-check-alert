#!/home/seil/program_tools/anaconda/anaconda2/bin/python

import smtplib
from sys import argv,exit
import pymongo as pm
import sys
from csv import writer
import datetime as dt
import time
import pandas as pd

user = "seil"
pwd = "seilers"

mail_list = ['rohitrgupta@gmail.com','karan.info3@gmail.com','hareesh.dx@gmail.com','cbahuman@gmail.com','bhushankadam410@gmail.com']#,'aanshul215@gmail.com']


actual_coll_list = ['power_k_m','power_k_f2_p','power_k_seil_a','power_k_dil_l','power_k_dil_a','temp_k_201','power_k_wc_a','power_k_ch_l','power_k_ch_a','power_k_sr_a','power_k_f2_l','temp_k_301','power_k_lab_od3','power_k_dil_p','power_k_wc_l','power_k_clsrm_ac2','power_k_cr_p','power_k_f2_a','power_k_p','power_k_a','power_k_cr_a','power_k_erts_p','power_k_fck_l','power_k_erts_l','power_k_lab_od2','power_k_off_a','power_k_seil_l','power_k_ch_p1','power_k_erts_a','power_k_off_l','power_k_ch_p2','power_k_clsrm_ac1','power_k_yc_p','power_k_clsrm_ac3','power_k_seil_p','power_k_wc_p','temp_k_213','power_k_lab_od1','power_k_fck_a','power_k_yc_a','power_k_sr_p']#,'temp_k_305','temp_k_205']

def init_mail():
        smtpserver = smtplib.SMTP("smtp.cse.iitb.ac.in",25)
        smtpserver.ehlo()
        smtpserver.starttls()
        smtpserver.ehlo()
        smtpserver.esmtp_features['auth'] = 'LOGIN DIGEST-MD5 PLAIN'

        return smtpserver


def close_mail_connection(smtpserver):
        smtpserver.close()


def send_alert_mail(to,body,subject):
        smtpserver = init_mail()
        smtpserver.login(user,pwd)
        header = "To:" + to + '\n' + 'From: ' + user +'@cse.iitb.ac.in\n' + 'Subject:'+subject+'\n'
        msg = header + '\n' + body + '\n\n'

        smtpserver.sendmail(user,to,msg)
        close_mail_connection(smtpserver)

def connect_mongo():
  db_mo = pm.MongoClient("10.129.23.41",27017)
  con = db_mo['data']  #new database
  return db_mo,con

#for displaying params column in mongo query
def get_param_dict(params):
  tempd = {}
  for param in params:
    tempd[param] = 1
  tempd['_id']= False
#   print tempd
  return tempd

params = ['TS']

if __name__ == '__main__':
  #print dt.datetime.now()
  #for mail_temp in mail_list:
    #send_alert_mail(mail_temp,"test","test")
  db_write_list = []

  try:
    mo_db, mo_con = connect_mongo() # connection to mongo db
  except:
    print("Unexpected error In Mongo Connection:", sys.exc_info()[0])
    tFile = str(time.strftime("%d-%m-%Y"))
    tWrite = time.strftime("%H:%M:%S", time.localtime(time.time()))
    with open("/home/seil/mongo_check_ALERT/error_" + tFile,"a+") as errF:
      errF.write(str(tWrite) + "\n" + str(sys.exc_info()[0]))


  try:
    collection_list = actual_coll_list
    param_dict = get_param_dict(params)
    last_mail_sent_ts = 0
    err_list = []
    list_dict = {}
    for key in actual_coll_list:
      print "%s -- Checking" %(key)
      ts_val_1 = int(list(mo_con[key].find({},param_dict).sort([('TS',-1)]).limit(1))[0]['TS'])
      ts_val_2 = int(dt.datetime.now().strftime('%s'))
      print key,ts_val_1,ts_val_2,ts_val_1-ts_val_2

      if ts_val_2 - ts_val_1 > 120:
        print "SMTHING'S NAAT GOOOOD"
        t_time = time.strftime("%H:%M:%S - %Y/%m/%d", time.localtime(ts_val_1))
        list_dict = {}
        list_dict['collection'] = key
        list_dict['row_TS'] = ts_val_1
        list_dict['HumanTS'] = t_time
        list_dict['TS'] = ts_val_2
        list_dict['Diff'] = ts_val_2 - ts_val_1
        db_write_list.append(list_dict)
#         print "dict inserted ",list_dict
#         print db_write_list
        err_dict = {key:t_time}
        err_list.append(err_dict)

    print "here is the list with problematic collections"
    print (err_list)
    if len(err_list) != 0:
      str_temp = ""
      for item in err_list:
        str_temp += str(item.keys()[0]) + "   ----   " + str(item.values()[0]) + "\n"

      for mail_temp in mail_list:
        body = "collection   ---   last_logged" + "\n" + str(str_temp)#"\n".join(err_list)
        body += "\n Please click on the link below to find out the list of (collection -- rpi_ip) mapping" + "\n"
        body += "https://docs.google.com/spreadsheets/d/1UJThvg8X6stxgA5w1f1C_RkyI7a96y9lBljy7ezzALo/edit#gid=0"
        subject = "Smartmeter/Temperature data logging issue(s) -- Kresit "
        send_alert_mail(mail_temp,body,subject)
        print "Mail Sent to " + mail_temp
  except:
    print("Unexpected error:", sys.exc_info()[0])
    tFile = str(time.strftime("%d-%m-%Y"))
    tWrite = time.strftime("%H:%M:%S", time.localtime(time.time()))
    with open("/home/seil/mongo_check_ALERT/error_" + tFile,"a+") as errF:
      errF.write(str(tWrite) + "\n" + str(sys.exc_info()[0]))

#   print db_write_list
  coll_name = 'alert'  
  flag = True
  while flag:
    try:
      record_id = mo_con[coll_name].insert_many(db_write_list)
      print "IIInsertedddddddddd ",record_id
      flag = False
    except:
      print "Mongo Insert Error"
      print("Unexpected error:", sys.exc_info()[0])
      print "trying to insert again"
      break

