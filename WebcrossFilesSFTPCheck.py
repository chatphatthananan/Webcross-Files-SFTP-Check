from importlib.metadata import files
import pysftp
import datetime
import config
from SGTAMProdTask import SGTAMProd
from config import SGTAM_log_config
import logging

if __name__ == '__main__':
  # setup logging
  logging.basicConfig(
      filename= f"log\{datetime.datetime.now().strftime('%Y%m%d%H%M')}_WebcrossFilesSFTPCheck.log",
      format='%(asctime)s %(levelname)s %(message)s',
      level=logging.INFO
  )

  s = SGTAMProd()
  config.SGTAM_log_config['statusFlag'], config.SGTAM_log_config['logID']  = s.insert_tlog(**config.SGTAM_log_config)

  logging.info("Android Version Scraper Started")

  # your script here
  try:
    logging.info("Process started.")
    hostname = 'xxx'
    user = 'xxx'
    pwd = 'xxx'
    cnOpts = pysftp.CnOpts() # to ignore known hosts check error
    cnOpts.hostkeys = None # to ignore known hosts check error
    files_and_dirs_list = []
    files_list = []
    logging.info('Establishing connection to SFTP.')
    with pysftp.Connection(host=hostname, username=user, password=pwd, cnopts=cnOpts) as sftp:
        logging.info("Connection is established.")
        logging.info("Navigating to '/opt/census_data/import/'.")
        sftp.cwd("../../opt/census_data/import/")
        #print(sftp.getcwd())
        logging.info('Listing all files and folders.')
        files_and_dirs_list = sftp.listdir()
        for f in files_and_dirs_list:
            if sftp.isfile('/opt/census_data/import/'+f):
                logging.info(f'File is detected: {f}')
                files_list.append(f)
    sftp.close()
    logging.info('SFTP connection has been closed.')

    # no stucked files, update tLog.
    if len(files_list) == 0:
        config.SGTAM_log_config['logMsg'] = "[OK] No webcross files detected in Census SFTP."
        logging.info(config.SGTAM_log_config['logMsg'])
    # detect stucked files, set Log, set email
    else:
        config.SGTAM_log_config['statusFlag'] = 2
        config.SGTAM_log_config['logMsg'] = "[ERROR] Webcross files might be stuck in Census SFTP."
        config.email['subject'] = "[ERROR] Webcross Files SFTP Check"
        config.email['body'] = f"There are webcross files found in Census SFTP, please check if they are stuck.\n{files_list}\n*This is an auto generated email, do not reply to this email."
        logging.warning(config.SGTAM_log_config['logMsg'])
        logging.warning("The following files are stuck on SFTP:")
        for f in files_list:
            logging.info(f)
  # exception detected during the process, set log and set email
  except Exception as e:
    config.SGTAM_log_config['statusFlag'] = 2
    config.SGTAM_log_config['logMsg'] = "[Error] There is/are exception(s), please check."
    config.email['subject'] = "[ERROR] Webcross Files SFTP Check"
    config.email['body'] = f"{config.SGTAM_log_config['logMsg']}\n{e}"
    logging.error(config.SGTAM_log_config['logMsg'])
    logging.error(e)
  # update tLog and send either warning or error email.
  finally:
    if config.SGTAM_log_config['statusFlag'] in [2,3]:
        s.send_email(**config.email)
        logging.info('Email sent.')
        s.update_tlog(**config.SGTAM_log_config)
        logging.info('SGTAM log updated.')
    else:
        s.update_tlog(**config.SGTAM_log_config)
        logging.info('SGTAM log updated.')

   
