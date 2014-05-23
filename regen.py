#!/usr/bin/env python3
import sqlite3
import html5lib
import json
import os
import re
import urllib.parse
import urllib.request
#import xml.etree.ElementTree as ET

PREFIX=os.environ.get('WPPREFIX', 'enwiki')
TIMEOUT=2*60 # seconds
imageconn = sqlite3.connect(PREFIX+'-images.db')
pageconn = sqlite3.connect(PREFIX+'-pages.db')

def apirequest(params):
    url = 'https://' + re.sub(r'wiki$', '', PREFIX) + '.wikipedia.org/w/api.php';
    params['format'] = 'json'
    params = urllib.parse.urlencode(params).encode('utf-8')
    with urllib.request.urlopen(url, params, TIMEOUT) as req:
        data = req.read().decode('utf-8')
        return json.loads(data)

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

def doit():
    totalfigs = 0
    totalregen = 0

    for row in pageconn.execute('SELECT name, figid, figure FROM figure'):
        name,figid,figurehtml = row
        document = html5lib.parse(figurehtml)
        figure = document.find('{http://www.w3.org/1999/xhtml}body')[0]
        img = figure.find('*/{http://www.w3.org/1999/xhtml}img')
        totalfigs += 1
        if img is None:
            print("Missing image!")
            print(figurehtml)
            continue
        ty = figure.get('typeof', '')
        if ty != 'mw:Image/Thumb':
            continue
        cl = figure.get('class', '').split()
        if 'mw-default-size' not in cl:
            continue
        # ok, this is an default-size thumb.  let's fetch the image size
        resource = img.get('resource')
        resource = re.sub(r'^([.][.]?/)+', '', resource)
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
                continue
            height = info['height']
            width = info['width']
            mediatype = info['mediatype']
        if height <= width:
            continue # not portrait
        # ok, this is a thumb to regenerate!
        totalregen += 1
        (filename,headers) = urllib.request.urlretrieve(thumburl(resource))
        st = os.stat(filename)
        os.remove(filename) # woot
        print("Generated", resource, st.st_size)

    print("=================")
    print("Total figures", totalfigs)
    print("Total regenerated", totalregen)

if __name__ == '__main__':
    doit()
