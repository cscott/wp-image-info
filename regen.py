#!/usr/bin/env python3
import sqlite3
import html5lib
import http.client
import json
import os
import re
import socket
import time
import urllib.parse
import urllib.request
#import xml.etree.ElementTree as ET

PREFIX=os.environ.get('WPPREFIX', 'enwiki')
AFTER=os.environ.get('WPAFTER', None)
PARALLEL = int(os.environ.get('WPWORKERS', '50'))
TIMEOUT = int(os.environ.get('WPTIMEOUT', 4*60)) # seconds
RETRIES = 4

def request(url, data=None):
    # errors I have seen:
    # urllib.error.HTTPError, urllib.error.URLError, socket.gaierror,
    # http.client.BadStatusLine
    for n in range(RETRIES):
        try:
            with urllib.request.urlopen(url, data, TIMEOUT) as req:
                return req.read()
        except (urllib.error.HTTPError, urllib.error.URLError,
                socket.gaierror, http.client.BadStatusLine):
            time.sleep(5*n)
        except:
            print("WHOO, A NEW ONE")
            time.sleep(5*n)
    return None

def apirequest(params):
    url = 'https://' + re.sub(r'wiki$', '', PREFIX) + '.wikipedia.org/w/api.php';
    params['format'] = 'json'
    params = urllib.parse.urlencode(params).encode('utf-8')
    data = request(url, params)
    if data is None: return None
    return json.loads(data.decode('utf-8'))

def imageinfo(resource, props, extradict=None):
    params = {
        'action': 'query',
        'prop': 'imageinfo',
        'titles': resource,
        'iiprop': '|'.join(props)
    }
    if extradict is not None:
        for k,v in extradict.items():
            params[k] = v
    resp = apirequest(params)
    if resp is None: return None
    try:
        info = [it for it in resp['query']['pages'].items()][0][1]['imageinfo']
        return info[0]
    except KeyError:
        return None

def thumburl(resource, size=220):
    info = imageinfo(resource, ['url'], {
        'iiurlwidth': size,
        'iiurlheight': size
    })
    if info is None or 'thumburl' not in info: return None
    return info['thumburl']

def allpages(apcontinue=None):
    CHUNK = 100
    while True:
        params = {
            'action': 'query',
            'list': 'allpages',
            'apnamespace': 0,
            'apfilterredir': 'nonredirects',
            'aplimit': CHUNK
        }
        if apcontinue is not None:
            params['apcontinue'] = apcontinue
        result = apirequest(params)
        qc = result.get('query-continue', {}).get('allpages', {})\
          .get('apcontinue', None)
        pages = result['query']['allpages']
        for p in pages:
            yield p
        if qc is None:
            break
        apcontinue = qc

def examinefigure(figure, imageconn=None):
    img = figure.find('*/{http://www.w3.org/1999/xhtml}img')
    if img is None:
        print("Missing image!")
        return False
    ty = figure.get('typeof', '')
    if ty != 'mw:Image/Thumb':
        return False
    cl = figure.get('class', '').split()
    if 'mw-default-size' not in cl:
        return False
    # ok, this is an default-size thumb.  let's fetch the image size
    resource = img.get('resource')
    resource = re.sub(r'^([.][.]?/)+', '', resource)
    info = None
    if imageconn is not None:
        info = imageconn.execute\
        ('SELECT width,height,mediatype FROM image WHERE name = ?',\
            (resource,) ).fetchone()
    if info is not None:
      (width,height,mediatype) = info
    else:
        # fetch from the MW API
        info = imageinfo(resource, ['size', 'mediatype'])
        if info is None:
            # try urldecode!
            resource = urllib.parse.unquote(resource)
            info = imageinfo(resource, ['size', 'mediatype'])
        if info is None:
            print("SKIPPING MYSTERIOUS IMAGE", resource)
            return False
        height = info['height']
        width = info['width']
        mediatype = info['mediatype']
    if height <= width:
        return False # not portrait
    # ok, this is a thumb to regenerate!
    url = thumburl(resource)
    if url is None:
        print("NO THUMB URL?", resource)
        return False
    thumbdata = request(url)
    if thumbdata is None:
        print("FAILED TO FETCH THUMB", url)
        return False
    print("Generated", resource, len(thumbdata))
    return True

def fetchparsoid(pageinfo, quiet=False):
    url = "http://parsoid-lb.eqiad.wikimedia.org/" + PREFIX + '/'
    url += urllib.parse.quote(pageinfo['title'])
    # XXX pageids returned by allpages are *not* revision ids
    #url += '?oldid=' + str(pageinfo['pageid'])
    data = request(url)
    if data is None:
        print('SKIPPING (parsoid error)', pageinfo['title'])
        return []
    document = html5lib.parse(data.decode('utf-8'))
    return document.findall('*//{http://www.w3.org/1999/xhtml}figure')

def doit_slow():
    totalfigs = 0
    totalregen = 0

    pageconn = sqlite3.connect(PREFIX+'-pages.db')
    imageconn = sqlite3.connect(PREFIX+'-images.db')
    for row in pageconn.execute('SELECT name, figid, figure FROM figure'):
        name,figid,figurehtml = row
        document = html5lib.parse(figurehtml)
        figure = document.find('{http://www.w3.org/1999/xhtml}body')[0]
        totalfigs += 1
        if examinefigure(figure, imageconn):
            totalregen += 1

    print("=================")
    print("Total figures", totalfigs)
    print("Total regenerated", totalregen)

def doit_slower():
    totalpages = 0
    totalfigs = 0
    totalregen = 0

    for p in allpages(AFTER):
        totalpages += 1
        print(" "*70, end='\r')
        print(totalpages, p['title'], end='\r')
        for figure in fetchparsoid(p):
            totalfigs += 1
            if examinefigure(figure):
                totalregen += 1

    print("")
    print("=================")
    print("Total pages", totalpages)
    print("Total figures", totalfigs)
    print("Total regenerated", totalregen)

def doit_fast():
    from queue import Queue
    from threading import Thread
    q = Queue(PARALLEL)
    results = Queue()
    def worker():
        while True:
            page = q.get()
            nfigs = 0
            nregen = 0
            for figure in fetchparsoid(page, quiet=True):
                nfigs += 1
                if examinefigure(figure):
                    nregen += 1
            results.put((page['title'], nfigs, nregen))
            q.task_done()
    def reporter():
        (totalpages, totalfigs, totalregen) = (0, 0, 0)
        last = time.time()
        while True:
            (title,nfigs,nregen) = results.get()
            totalpages += 1
            totalfigs += nfigs
            totalregen += nregen
            if time.time() > (last + 0.5):
                print(" "*70, end='\r')
                print(PREFIX, totalpages, totalfigs, totalregen, p['title'], end='\r')
                last = time.time()
            results.task_done()
    for i in range(PARALLEL): # number of worker threads
        t = Thread(target=worker)
        t.daemon = True
        t.start()
    t = Thread(target=reporter)
    t.daemon = True
    t.start()

    for p in allpages(AFTER):
        q.put(p)
    q.join() # block until all tasks are done

    print("")

if __name__ == '__main__':
    which = os.environ.get('WPWHICH', 'fast')
    if which == 'slow':
        doit_slow()
    elif which == 'slower':
        doit_slower()
    else:
        doit_fast()
