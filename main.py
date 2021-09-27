import pyodbc
import requests
import keyring
import json
import logging
from apscheduler.schedulers.blocking import BlockingScheduler

# log configuration
logging.basicConfig(filename='log_file.log',
                    format='%(levelname)s -- %(asctime)s -- %(funcName)s::%(lineno)d -- %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)


def algo(main_ls2, num2, count2, mssg):
    result_ls = main_ls2[num2:]
    keyring.set_password("realtime_mssql", "last_enroll_count", str(count2))
    url = 'http://oswalhr.com/Attendence/add_machine_attendence_data'
    req = requests.post(url, data=json.dumps(result_ls))
    logging.info(f"Response: {req}")


def main():
    try:
        try:
            cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=LAPTOP-JCKNUMM7\SQLEXPRESS;DATABASE=AttDB;UID=rss;PWD=abc@123')
            cursor = cnxn.cursor()

            cursor.execute("SELECT tbl_realtime_glog.update_time,tbl_realtime_glog.user_id,tbl_realtime_glog.device_id FROM "
                           "tbl_realtime_glog")
        except:
            logging.error("Database Connection Error")
            return

        try:
            num = int(keyring.get_password("realtime_mssql", "last_enroll_count"))
        except:
            num = 0

        main_ls = []
        count = 0
        for row in cursor.fetchall():
            ls = []
            count += 1
            for cell in row:
                if cell == row[0]:
                    day = int(cell.strftime("%d"))
                    month = int(cell.strftime("%m"))
                    year = int(cell.strftime("%Y"))
                    date = f"{day}/{month}/{year}"
                    ls.append(date)
                    time = cell.strftime("%H:%M")
                    ls.append(time)
                else:
                    ls.append(str(cell))
            main_ls.append(ls)

        if count > num:
            mssg = f"Total: {count}, Updating: {count - num}"
            try:
                algo(main_ls, num, count, mssg)
            except:
                logging.critical("Error while updating data to server")
                logging.exception("Error")
            logging.info(mssg)
            logging.info(f"Data: {main_ls[num:]}")
        elif count < num:
            num = 0
            mssg = f"Data Reset::DB Count: {count}, Updated Count:{num}"
            keyring.set_password("realtime_mssql", "last_enroll_count", str(0))
            logging.error(mssg)
            logging.error(f"Data: {main_ls}")
        else:
            mssg = "No updated data"
            logging.info(f"{mssg}")
    except:
        logging.critical(f"Function Error")
        logging.exception(f"Function Error")


scheduler = BlockingScheduler()
scheduler.add_job(main, 'interval', seconds=10)
scheduler.start()