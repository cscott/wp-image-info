#!/usr/bin/env python3
import sqlite3
import html5lib
import json
import re
#import xml.etree.ElementTree as ET

PREFIX='frwiki'
imageconn = sqlite3.connect(PREFIX+'-images.db')
pageconn = sqlite3.connect(PREFIX+'-pages.db')

PRINT_BOGOSITY=False
BINSIZE = 5

totalfigs = 0
totalupright = 0
totaluprightsize = 0
totalchanged075 = 0
totalchanged100 = 0
totalbogus = 0

hist075 = []
hist100 = []

def incrhist(h, v):
    b = int(v / BINSIZE)
    while len(h) <= b:
        h.append(0)
    h[b] += 1
def printhist(h):
    i = 0
    width = max(h)
    xscale = min(1, 60/max(h))
    while i < len(h):
        v = int(h[i]*xscale)
        label = str(h[i]) if h[i] > 0 else ''
        print('{:3d} {:s} {:s}'.format(i*BINSIZE, ''.ljust(v,'x'), label))
        i += 1

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
    dp = json.loads(figure.get('data-parsoid', '{}'))
    optList = dp['optList'] or []
    if PRINT_BOGOSITY:
      # look at bogus options.  sadly, conflicting format options don't show
      # up as bogus
      bogosity = [item['ak'] for item in optList \
                  if item['ck'] == 'bogus' and item['ak'].strip() != '']
      if len(bogosity) == 0:
        continue
      print(bogosity)
      totalbogus += 1
      continue
    # look at the 'upright' options.
    upright = [item['ak'] for item in optList if item['ck']=='upright']
    if len(upright) == 0:
        continue
    totalupright += 1
    # is there also an explicit width?
    size = [item['ak'] for item in optList if item['ck']=='width']
    if len(size) != 0:
        print("Warning: upright and size!", optList)
        totaluprightsize += 1
        continue
    # what's the upright factor?
    factor = upright[0].rsplit('=', 1)
    if len(factor) < 2:
        factor = 0.75 # default factor
        nfactor = 1.00 # new default factor
    else:
        try:
            factor = float(factor[1])
            nfactor = factor
        except ValueError:
            # something crazy like 'upright=300px'
            totalbogus += 1
            continue
    # ok, this is an upright image.  let's fetch the image size
    resource = img.get('resource')
    resource = re.sub(r'^([.][.]?/)+', '', resource)
    info = imageconn.execute\
      ('SELECT name,pageid,width,height,mediatype FROM image WHERE name = ?',\
        (resource,) ).fetchone()
    if info is None:
        # XXX fetch from the MW API
        continue
    (figname,pageid,width,height,mediatype) = info
    # compute old size
    oldwidth = 180 * factor
    # compute new size
    newwidth075 = width * ((180 *  factor) / max(width,height))
    newwidth100 = width * ((180 * nfactor) / max(width,height))
    # difference
    diff075 = abs(oldwidth - newwidth075)
    diff100 = abs(oldwidth - newwidth100)
    incrhist(hist075, diff075)
    incrhist(hist100, diff100)
    if diff075 > 5:
        totalchanged075 += 1
    if diff100 > 5:
        totalchanged100 += 1
    if diff075 > 5 and diff100 > 5:
        print("Changed",name,"figure",resource,"was",oldwidth,"is",newwidth075,"or",newwidth100)
    #if totalchanged100 > 5:
    #    break # xXX

print("=================")
print("Total figures", totalfigs)
print("Total bogus", totalbogus)
if not PRINT_BOGOSITY:
    print("Total upright", totalupright)
    print("Total upright *and* size", totaluprightsize)
    print("Total changed (0.75)", totalchanged075)
    printhist(hist075)
    print("Total changed (1.00)", totalchanged100)
    printhist(hist100)
