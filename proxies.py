# -*- coding: utf-8 -*-
import asyncio
from proxybroker import Broker
from random import shuffle

async def save(proxies, filename):
    """Save proxies to a file."""
    with open(filename, 'w') as f:
        while True:
            proxy = await proxies.get()
            if proxy is None:
                break
            proto = 'https' if 'HTTPS' in proxy.types else 'http'
            row = '%s://%s:%d\n' % (proto, proxy.host, proxy.port)
            f.write(row)


def get_proxies(work_dir, n):
    proxies = asyncio.Queue()
    broker = Broker(proxies, timeout=6)
    tasks = asyncio.gather(broker.find(types=['HTTP', 'HTTPS'], limit=n,countries=['RU','UA']),
                           save(proxies, filename=work_dir+'/proxies.txt'))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)

    with open(work_dir+'/proxies.txt', 'r') as prx:
        proxies_list=prx.read().split('\n')
        prx.close()

    return proxies_list
