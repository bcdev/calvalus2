#!/usr/bin/env python
#!/home/hadoop/opt/anaconda3/bin/python -u
#!/usr/bin/python -u

"""Tool to download a product from a DHuS server"""

__author__ = "Martin Boettcher, Brockmann Consult GmbH"
__copyright__ = "Copyright 2017, Brockmann Consult GmbH"
__license__ = "For use with Calvalus processing systems"
__version__ = "1.0"
__email__ = "info@brockmann-consult.de"
__status__ = "Production"

# changes in 1.1
# ...

import sys
import os
import time
import datetime
import pytz
import urllib2
import hashlib
import traceback
import subprocess
from lxml import etree
from urlparse import urlsplit

# dhusget.py thomas.boettcher s2i.report https://scihub.copernicus.eu/apihub/odata/v1/Products data/S2A_MSIL1C_20171213T140051_N0206_R067_T21JTM_20171213T171822.zip

if len(sys.argv) < 2:
    print('Usage:')
    print('  dhusget.py <user> [<reportfile>] <baseurl> <destdirandfile>')
    print('e.g.')
    print("  dhusget.py martin.boettcher s2i.report https://scihub.copernicus.eu/apihub/odata/v1/Products data/S2A_MSIL1C_20171213T140051_N0206_R067_T21JTM_20171213T171822.zip")
    sys.exit(1)

srcuser = sys.argv[1] if len(sys.argv) > 2 else 'martin.boettcher'
report = sys.argv[2] if len(sys.argv) > 3 else None
srchost = urlsplit(sys.argv[-2]).netloc
srcroot = urlsplit(sys.argv[-2]).path
target = sys.argv[-1]

execfile('.'+srcuser)

timeout = None
attempts = 12

namespaces = { 'a' : 'http://www.w3.org/2005/Atom',
               'o' : 'http://a9.com/-/spec/opensearch/1.1/',
               'm' : 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
               'd' : 'http://schemas.microsoft.com/ado/2007/08/dataservices'}

# read stored cookie for user and host

cookie_filename = '.' + srcuser + '-' + srchost + '-cookie'
try:
    with open(cookie_filename, 'r') as file:
        cookie = file.read()
except:
    cookie = None

# determine product base name from command line parameter

name = target[target.rfind('/')+1:]
if name.endswith('.zip'):
    name = name[:-4]
if name.endswith('.tar'):
    name = name[:-4]
if name.endswith('.SAFE'):
    name = name[:-5]
if name.endswith('.SEN3'):
    name = name[:-5]

# create target directory

pos = target.rfind('/')
if pos > 0 and not os.path.exists(target[:pos]):
    os.makedirs(target[:pos])

# attempt loop

cycle = 0
while cycle <= attempts:
    try:
        # install opener, set cookie

        dataAuthHandler = urllib2.HTTPBasicAuthHandler()
        dataAuthHandler.add_password(realm='OData service', uri='https://'+srchost+srcroot, user=srcuser, passwd=srcpassword)
        opener = urllib2.build_opener(dataAuthHandler)
        if cookie:
            opener.addheaders.append(('Cookie', cookie))
        urllib2.install_opener(opener)

        # inquire uid for product base name, maybe store cookie

        query = 'https://' + srchost + srcroot + "?select=Id&$filter=Name eq '" + name + "'"
        query = query.replace(' ', '%20')
        print(query)
        response = urllib2.urlopen(query, timeout=timeout)
        cookie = response.headers.get('Set-Cookie')
        if cookie:
            with open(cookie_filename, "w") as file:
                file.write(cookie)
            print('cookie stored: ' + cookie)
        xml = response.read()
        #print(xml)
        dom = etree.fromstring(xml)
        id = dom.xpath('/a:feed/a:entry/a:id', namespaces=namespaces)[0].text

        # submit checksum query

        url = id + '/Checksum'
        print(url)
        response = urllib2.urlopen(url, timeout=timeout)
        xml = response.read()
        #print(xml)
        dom = etree.fromstring(xml)
        checksum = dom.xpath('/d:Checksum/d:Value', namespaces=namespaces)[0].text.lower()
        print(checksum)

        # submit data query

        url = id + '/$value'
        print(url)
        response = urllib2.urlopen(url, timeout=timeout)
        if not 'content-length' in response.headers:
            print('response does not contain binary as expected:')
            print(response.read())
            raise IOError
        size = int(response.headers['Content-Length'])

        # read data and dump it to file

        pos = 0
        start_time = datetime.datetime.now(pytz.timezone('UTC'))
        md5accu = hashlib.md5()
        with open(target, 'wb') as file:
            while (True):
                block = response.read(8192)
                # end of content, break loop
                if len(block) == 0:
                    if pos < size:
                        print('download ends prematurely at ' + str(pos) + '/' + str(size))
                        raise IOError
                    break
                file.write(block)
                pos += len(block)
                md5accu.update(block)
                # determine initial data rate after some blocks
                if pos == 4096 * 8192:
                    now = datetime.datetime.now(pytz.timezone('UTC'))
                    delta = now - start_time
                    seconds = delta.days * 86400 + delta.seconds
                    if seconds > 0:
                        datarate = int(pos * 8 / 1024 / seconds)
                        print("initial data rate[kbps]: " + str(datarate))

        # determine final data rate

        now = datetime.datetime.now(pytz.timezone('UTC'))
        delta = now - start_time
        seconds = delta.days * 86400 + delta.seconds
        if seconds > 0:
            datarate = int(pos * 8 / 1024 / seconds)
            print("overall data rate[kbps]: " + str(datarate))
        else:
            datarate = 0

        # check md5 sum

        md5sum = md5accu.hexdigest()
        if md5sum != checksum:
            if cycle < 1:
                print('checksum error, ' + md5sum + ' instead of ' + checksum)
                raise IOError
            md5sum = subprocess.check_output(['md5sum', target]).split()[0]
            if md5sum != checksum:
                if cycle < 2:
                    print('checksum error, ' + md5sum + ' instead of ' + checksum)
                    raise IOError
                if subprocess.call(['unzip', '-t', target]) != 0:
                    print('unzip ' + target + ' failed')
                    raise IOError
                print('unzip test passed, ignoring failed checksum test')
            else:
                print('checksum ' + md5sum + ' test passed')
        else:
            print('checksum ' + md5sum + ' test passed')

        # write report line

        with open(target + '.rep', 'w') as file:
            file.write(name + '\t' + str(size) + '\t' + str(datarate) + '\t' + now.strftime('%Y-%m-%dT%H:%M:%SZ') + '\n')

        # exit with success

        print(target + " retrieved")
        sys.exit(0)
    except SystemExit:
        raise
    except IOError:
        traceback.print_exc(file=sys.stdout)
        cycle = 2
    except:
        traceback.print_exc(file=sys.stdout)
        pass

    # retry immediately or later with delay
    if cycle < 1:
        print("retrying ...")
    else:
        print("waiting for retry ...")
        time.sleep(300)
    cycle += 1

# exit with failure

sys.exit(1)

