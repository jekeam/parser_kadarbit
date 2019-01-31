import time
import requests
from bs4 import BeautifulSoup
import sys
import pandas as pd
import datetime
import re
import imaplib
import email
import os
import base64
import json
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
import smtplib
from proxies import get_proxies
import multiprocessing as mp
#%%
kad_head={
'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding':'gzip, deflate',
'Accept-Language':'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
'Cache-Control':'max-age=0',
'Connection':'keep-alive',
'Host':'kad.arbitr.ru',
'Upgrade-Insecure-Requests':'1',
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

captcha_head={
'Accept':'application/json, text/javascript, */*',
'Accept-Encoding':'gzip, deflate',
'Accept-Language':'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
'Connection':'keep-alive',
'Host':'kad.arbitr.ru',
'Referer':'http://kad.arbitr.ru/',
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
'X-Requested-With':'XMLHttpRequest'}

validation_head={
'Accept':'application/json, text/javascript, */*',
'Accept-Encoding':'gzip, deflate',
'Accept-Language':'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
'Connection':'keep-alive',
'Content-Type':'application/x-www-form-urlencoded',
'Host':'kad.arbitr.ru',
'Referer':'http://kad.arbitr.ru/',
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
'X-Requested-With':'XMLHttpRequest'}

search_head={
'Accept':'*/*',
'Accept-Encoding':'gzip, deflate',
'Accept-Language':'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
'Connection':'keep-alive',
'Content-Type':'application/json',
'Host':'kad.arbitr.ru',
'Origin':'http://kad.arbitr.ru',
'RecaptchaToken':'e1c9fbbd-5d33-4954-9fec-a01dfd0ce546',
'Referer':'http://kad.arbitr.ru/',
'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
'x-date-format':'iso',
'X-Requested-With':'XMLHttpRequest'}
#%%

def get_proxies_from_file(work_dir):
    with open(work_dir+'proxieslist.txt', 'r') as prx:
        proxies_list=prx.read().split('\n')
        prx.close()

    return iter([{'http':prx} for prx in proxies_list])

def log(message):
    print(message)
    with open(os.path.join(WORK_DIR,'cad_arbit_log'), 'a') as log_file:
        log_file.write('{} {}\n'.format(str(datetime.datetime.now()), message))
        log_file.close()

def requests_control(session, case, url, head, req_type, data=None):
    global PROXY,PROXIES_GEN
    while True:
        try:
            try:
                response=session.get(url, headers=head, data=data, proxies=PROXY, timeout=5)
                while (response.status_code != 200) or (req_type == 1 and response.status_code == 403) or (req_type == 2 and case not in response.text) or (req_type == 3 and response.status_code == 200 and 'json' not in response.headers['Content-Type']):
                    PROXY=next(PROXIES_GEN)
                    log('DEAD PROXY')
                    response=session.get(url, headers=head, data=data, proxies=PROXY, timeout=5)
                    break
                break
            except Exception as e:
                PROXY=next(PROXIES_GEN)
                log('DEAD PROXY')
                response=requests_control(session, case, url, head, req_type, data)
                break
        except StopIteration:
            log('PROXY LIST IS EMPTY')
            time.sleep(600)
            PROXIES_GEN = get_proxies_from_file(WORK_DIR)
    return response

def send_mail(send_to, subject, text, files=None):
    msg = MIMEMultipart()
    msg['From'] = CRAWLER_EMAIL
    msg['To'] = send_to
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=os.path.basename(f)
            )
        part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.ehlo()
    smtp.starttls()
    smtp.login(CRAWLER_EMAIL, CRAWLER_PASSWORD)
    smtp.sendmail(CRAWLER_EMAIL, send_to, msg.as_string())
    smtp.close()

def parse_file(file_name, file_receiver):
    global PROXY
    filename=os.path.join(WORK_DIR, file_name)
    log('BEGIN PARSING')
    try:
        case_numbers=pd.read_excel(filename).drop_duplicates() 
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        log('ERROR WITH READING FILE: {} ({})'.format(str(e), tb.tb_lineno))
        sys.exit('FATAL ERROR')
    
    session=requests.Session()
    
    for i in case_numbers.index:
        case=case_numbers['Номер дела'][i].upper().replace('\\','/')
        log(case)
        try:
            filter_json='{"Page":1,"Count":25,"Courts":[],"DateFrom":null,"DateTo":null,"Sides":[],"Judges":[],"CaseNumbers":["%s"],"WithVKSInstances":false}' % (case)
            res=requests_control(session,case, 'http://kad.arbitr.ru/Kad/SearchInstances',search_head, req_type=1, data=filter_json.encode('utf8'))
            attempts=10
            skip=0
            while True:
                try:
                    res_soup=BeautifulSoup(res.text, 'lxml')
                    if int(res_soup.find('input',{'id':'documentsPagesCount'})['value']) == 0:
                        skip=1
                        break
                    case_url=res_soup.find('a',{'class':'num_case'})['href']
                    break
                except Exception as e:
                    PROXY=next(PROXIES_GEN)
                    log('BAD PROXY')
                    attempts-=1
                    if attempts == 0:
                        raise e
            if skip:
                continue            
        except Exception as e:
            exc_type, exc_obj, tb = sys.exc_info()
            exc_type, exc_obj, tb = sys.exc_info()
            log('ERROR WITH PARSING CASE LINK: {} ({})'.format(str(e), tb.tb_lineno))
        else:
            try:
                case_page=requests_control(session, case, case_url, kad_head, req_type=2)
                attempts=10
                while True:
                    try:
                        case_soup=BeautifulSoup(case_page.text, 'lxml')
                        insts=case_soup.find('div', {'id':'chrono_list_content'}).find_all('div', {'data-id':True})
                        break
                    except Exception as e:
                        PROXY=next(PROXIES_GEN)
                        log('BAD PROXY')
                        attempts-=1
                        if attempts == 0:
                            raise e
            except Exception as e:
                exc_type, exc_obj, tb = sys.exc_info()
                log('ERROR WITH PARSING CASE PAGE: {} ({})'.format(str(e), tb.tb_lineno))
            else:                
                content={}
                try:
                    for inst in insts:
                
                        inst_resp=requests_control(session, case, 'http://kad.arbitr.ru/Kad/InstanceDocumentsPage?_={}&id={}&caseId={}&withProtocols=true&perPage=30&page=1'.format(int(datetime.datetime.timestamp(datetime.datetime.now())), inst['data-id'], case_url.replace('http://kad.arbitr.ru/Card/','')),validation_head, req_type=3)
                        attempts=10
                        while True:
                            try:
                                inst_json=inst_resp.json()
                                break
                            except Exception as e:
                                PROXY=next(PROXIES_GEN)
                                log('BAD PROXY')
                                attempts-=1
                                if attempts == 0:
                                    raise e
                        chrnlg=['[{}] - {} \\. \\'.format(cinst['DisplayDate'], ';'.join(cinst['ContentTypes'])) for cinst in inst_json['Result']['Items']]
                
                        while inst_json['Result']['PagesCount'] > inst_json['Result']['Page']:
                            inst_resp=requests_control(session, case, 'http://kad.arbitr.ru/Kad/InstanceDocumentsPage?_={}&id={}&caseId={}&withProtocols=true&perPage=30&page={}'.format(int(datetime.datetime.timestamp(datetime.datetime.now())), inst['data-id'], case_url.replace('http://kad.arbitr.ru/Card/',''), inst_json['Result']['Page']+1), validation_head, req_type=3)
                            attempts=10
                            while True:
                                try:
                                    inst_json=inst_resp.json()
                                    break
                                except Exception as e:
                                    PROXY=next(PROXIES_GEN)
                                    log('BAD PROXY')
                                    attempts-=1
                                    if attempts == 0:
                                        raise e
                            chrnlg+=['[{}] - {} \\. \\'.format(cinst['DisplayDate'], cinst['DecisionTypeName'] or ';'.join(cinst['ContentTypes'])) for cinst in inst_json['Result']['Items']]
                
                #        chrnlg+=['[{}] - {} \\. \\'.format(inst.find('span', {'class':'b-reg-date'}).text,re.sub('[\n\r\t]', '', [chldrn for chldrn in inst.find('div',{'class':'r-col'}).children if chldrn.name][1].text).strip())]
                        chrnlg+=['[{}] - {} \\. \\'.format('Статус',re.sub('[\n\r\t]', '', [chldrn for chldrn in inst.find('div',{'class':'r-col'}).children if chldrn.name][1].text).strip())]
                
                        content[re.sub('[\n\r\t]', '', inst.find('strong').text).strip()]='\n\n'.join(chrnlg)
                except Exception as e:
                    exc_type, exc_obj, tb = sys.exc_info()
                    log('ERROR WITH CONTENT PARSING: {} ({})'.format(str(e), tb.tb_lineno))
                else:
                    case_numbers.at[i, 'Ссылка для просмотра онлайн']=case_url
                    case_numbers.at[i, 'Данные суда первой инстанции']=content.get('Первая инстанция','')
                    case_numbers.at[i, 'Данные суда аппеляции']=content.get('Апелляционная инстанция','')
                
    try:
        case_numbers.to_excel(os.path.join(WORK_DIR,'outfile.xlsx'), index=None)
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        log('ERROR WITH SAVING RESULTS: {} ({})'.format(str(e), tb.tb_lineno))
    else:
        log('DATA COLLECTED')
        try:
            send_attempts=2
            while send_attempts > 0:
                try:
                    send_mail(file_receiver, 'KAD ARBITR CRAWLER', 'Result', [os.path.join(WORK_DIR,'outfile.xlsx')])
                    break
                except Exception as e:
                    if 'Server busy, try again later' in str(e):
                        log('ERROR WITH SENDING RESULTS: Server busy')
                        send_attempts-=1
                        time.sleep(180)
                    else:
                        raise e
            if send_attempts == 0:
                raise Exception('Server busy')
        except Exception as e:
            exc_type, exc_obj, tb = sys.exc_info()
            log('ERROR WITH SENDING RESULTS: {} ({})'.format(str(e), tb.tb_lineno))
        else:
            log('RESULTS SENT')
            
    try:
        os.remove(filename)
        queue=json.load(open(WORK_DIR+'queue.json', 'r'))
        files_queue=queue['files']
        files_queue=[f for f in files_queue if f[0] != file_name]
        json.dump({"files":files_queue}, open(WORK_DIR+'queue.json', 'w'))
    except:
        pass

#%%        
CRAWLER_EMAIL='kadarbitrcrawler@gmail.com'
CRAWLER_PASSWORD='AAaa123456789'
WORK_DIR='/home/parser/'
#%%
if __name__ == '__main__':
    log('Start')    
        
    files=[]
    try:
        imapSession = imaplib.IMAP4_SSL('imap.gmail.com',993)
        imapSession.login(CRAWLER_EMAIL,CRAWLER_PASSWORD)
        imapSession.select('inbox')
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        log('ERROR WITH CONNECTING TO MAILBOX: {} ({})'.format(str(e), tb.tb_lineno))
        sys.exit('FATAL ERROR')
    try:
        typ, data = imapSession.search(None, 'ALL')
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        log('ERROR WITH EXTRACTING MAILS: {} ({})'.format(str(e), tb.tb_lineno))
    
    if typ=='OK':
        mids=data[0].split()
        for i in mids:
            try:
                typ, bmsg = imapSession.fetch(i, '(RFC822)' )
                if typ=='OK':
                    for response_part in bmsg:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_string(response_part[1].decode('utf8'))
                            for part in msg.walk():
                                try:
                                    if part.get_content_maintype() == 'multipart':
                                        continue
                                    if part.get('Content-Disposition') is None:
                                        continue
                                    filename =  base64.b64decode(part.get_filename()[10:]).decode("utf-8") if '?utf-8?' in part.get_filename() else part.get_filename()
                                    tid=int(datetime.datetime.now().timestamp())
                                    if '.xlsx' in filename.lower() and 'outfile' not in filename.lower():
                                        fp = open(os.path.join(WORK_DIR,'infile_{}.xlsx'.format(tid)), 'wb')
                                        fp.write(part.get_payload(decode=True))
                                        fp.close()
                                        log('FILE RECEIVED: '+filename)
                                        files.append(['infile_{}.xlsx'.format(tid), re.findall('(\w*@\w*.\w*)',msg['From'])[0] if '?utf-8?' in msg['From'] else msg['From']])
                                        queue=json.load(open(WORK_DIR+'queue.json', 'r'))
                                        files_queue=queue['files']
                                        files_queue=files+files_queue
                                        json.dump({"files":files_queue}, open(WORK_DIR+'queue.json', 'w'))
                                except Exception as e:
                                    exc_type, exc_obj, tb = sys.exc_info()
                                    log('ERROR WITH PROCESSING INFILE: {} ({})'.format(str(e), tb.tb_lineno))
            except Exception as e:
                exc_type, exc_obj, tb = sys.exc_info()
                log('ERROR WITH DOWNLOADING INFILE: {} ({})'.format(str(e), tb.tb_lineno))
            else:
                try:
                    imapSession.store(i, '+FLAGS', '\\Deleted')
                except:
                    pass
    try:
        imapSession.close()
        imapSession.logout()
    except:
        pass
    
    #%%
        
    queue=json.load(open(WORK_DIR+'queue.json', 'r'))
    files=queue['files']
    if len(files) > 0:
        log('PROXY SEARCH')
        PROXIES_GEN = get_proxies_from_file(WORK_DIR)
        PROXY = next(PROXIES_GEN)
    else:
    	log('Wait 5 min...')
    	time.sleep(60*5)
    
    #%%
    for f in files:
        parse_file(f[0], f[1])
    log('Exit')
