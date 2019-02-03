from proxies import get_proxies
import multiprocessing as mp
import requests
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

def check_proxy(proxies_for_check, valid_proxies):
    for prx in proxies_for_check:
        try:
            requests.get('http://kad.arbitr.ru', headers=kad_head, proxies={'http':prx}, timeout=3.51)
            valid_proxies.append(prx)
        except:
            pass

def check_proxies(proxies_list):
    mgr=mp.Manager()
    valid_proxies_list=mgr.list()
    
    n_chunks=10
    chunks=[proxies_list[i::n_chunks] for i in range(n_chunks)]

    prcs=[]
    for chunk in chunks:
        p=mp.Process(target=check_proxy, args=(chunk,valid_proxies_list))
        prcs.append(p)
        p.start()
    
    for p in prcs:
        p.join()
        
    return valid_proxies_list
#%%
if __name__ == '__main__':
    WORK_DIR='/home/parser/'
    
    pl = get_proxies(WORK_DIR, 300)

    pl=check_proxies(pl)
    
    try:
        with open(WORK_DIR+'proxieslist.txt', 'r') as prx:
            proxies_list=prx.read().split('\n')
            prx.close()
    except:
        proxies_list=None
    
    if proxies_list:
        pl.extend(proxies_list)
    
    pl=list(set(pl))
    
    with open(WORK_DIR+'proxieslist.txt', 'w') as prx:
        prx.write('\n'.join(pl))
        prx.close()
