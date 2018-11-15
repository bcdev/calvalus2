#!/usr/bin/env python
#!/home/hadoop/opt/miniconda2/bin/python -u
#!/usr/bin/python -u

from __future__ import print_function

"""Tool to search for products in a DHuS server"""

__author__ = "Martin Boettcher and Thomas Storm, Brockmann Consult GmbH"
__copyright__ = "Copyright 2017, Brockmann Consult GmbH"
__license__ = "For use with Calvalus processing systems"
__version__ = "1.0"
__email__ = "info@brockmann-consult.de"
__status__ = "Production"

# changes in 1.1
# ...

import sys
import urllib2
import traceback
from lxml import etree
import urlparse

# dhussearch.py thomas.boettcher https://scihub.copernicus.eu/apihub/search S2MSI2Ap 2018-01-01T00:00:00.000Z 2018-01-02T00:00:00.000Z 'POLYGON((5.98865807458 47.3024876979,5.98865807458 54.983104153115,15.0169958839 54.983104153115,15.0169958839 47.3024876979,5.98865807458 47.3024876979))'
# dhussearch.py thomas.boettcher https://scihub.copernicus.eu/apihub/search S2MSI1C 2018-01-01T00:00:00.000Z 2018-01-02T00:00:00.000Z T32UMF,T32UNF,T32UME

class DhusSearch:
    namespaces = {'a': 'http://www.w3.org/2005/Atom',
                  'o': 'http://a9.com/-/spec/opensearch/1.1/',
                  'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
                  'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'}
    timeout = None
    attempts = 12
    cookie = None

    def search(self, srcuser, srcpassword, src_base_url, product_type, start, stop, area):

        search_base_url = src_base_url.replace('odata/v1/Products', 'search')
        condition = (
            'q=' + product_type +
            ' AND ' + 'beginposition' + ':[' + start +
            ' TO ' + stop + ']' +
            self._area_condition_of(area)
            )
        srchost = urlparse.urlsplit(src_base_url).netloc

        # read stored cookie for user and host

        cookie_filename = '.' + srcuser + '-' + srchost + '-cookie'
        try:
            with open(cookie_filename, 'r') as file:
                self.cookie = file.read()
        except:
            self.cookie = None

        # install opener, set cookie
        
        dataAuthHandler = urllib2.HTTPBasicAuthHandler(urllib2.HTTPPasswordMgrWithDefaultRealm())
        dataAuthHandler.add_password(realm=None, uri=search_base_url, user=srcuser, passwd=srcpassword)
        opener = urllib2.build_opener(dataAuthHandler)
        if self.cookie:
            opener.addheaders.append(('Cookie', self.cookie))
        urllib2.install_opener(opener)

        # attempt loop
        
        cycle = 0
        while cycle <= self.attempts:
            try:
                result = []
                row = 0
                hits = 0
                while True:
                    query = search_base_url + '?' + condition + '&rows=' + '20' + '&start=' + str(row)
                    query = query.replace(' ', '%20')
                    hits = self._incremental_search(query, result)
                    row += 20
                    if row >= hits:
                        return result
            except SystemExit:
                raise
            except IOError:
                traceback.print_exc(file=sys.stderr)
                cycle += 1
            except:
                traceback.print_exc(file=sys.stderr)
                cycle += 1


    def _urlopen(self, url_string, timeout=None):
        if self.cookie is not None:
            # use session cookie from previous calls ...
            url = urllib2.Request(url_string)
            url.add_header('cookie', self.cookie)
            try:
                response = urllib2.urlopen(url, timeout=timeout)
                return response
            except HTTPError as e:
                self.cookie = None
        # ask for new cookie with string url without cookie ...
        response = urllib2.urlopen(url_string, timeout=timeout)
        self.cookie = response.headers.get('Set-Cookie')
        return response


    def _incremental_search(self, query, accu):
        try:
            xml = None
            response = self._urlopen(query)
            xml = response.read()
            # print(xml)
            dom = etree.fromstring(xml)
            hits = int(dom.xpath('/a:feed/o:totalResults', namespaces=self.namespaces)[0].text)
            for i in dom.xpath('/a:feed/a:entry', namespaces=self.namespaces):
                name = i.xpath('a:str[@name="identifier"]', namespaces=self.namespaces)[0].text
                accu.append(name)
            return hits
        except:
            print("search error: " + str(sys.exc_info()[0]), file=sys.stderr)
            print("query: " + str(query), file=sys.stderr)
            print("response: " + str(xml), file=sys.stderr)
            raise


    def _area_condition_of(self, area):
        if area.startswith('POLYGON') or area.startswith('POINT') or area.startswith('MULTIPOINT'):
            return ' AND footprint:"Intersects(' + area + ')"'
        granules = area.split(',')
        if len(granules) == 1:
            return ' AND filename:*' + granules[0] + '*'
        condition = ' AND (filename:*' + granules[0] + '*'
        for granule in granules[1:]:
            condition = condition + ' OR filename:*' + granule + '*'
        return condition + ')'


if __name__ == '__main__':

    if len(sys.argv) < 7:
        print('Usage:')
        print('  dhussearch.py <user> <baseurl> <producttype> <start> <stop> <area>')
        print('e.g.')
        print("  dhussearch.py martin.boettcher https://scihub.copernicus.eu/apihub/search S2MSI2Ap 2018-01-01T00:00:00.000Z 2018-01-02T00:00:00.000Z 'POLYGON((5.98865807458 47.3024876979,5.98865807458 54.983104153115,15.0169958839 54.983104153115,15.0169958839 47.3024876979,5.98865807458 47.3024876979))'")
        print("  dhussearch.py martin.boettcher https://scihub.copernicus.eu/apihub/search S2MSI2Ap 2018-01-01T00:00:00.000Z 2018-01-02T00:00:00.000Z T32UMF,T32UNF,T32UME")
        sys.exit(1)

    srcuser = sys.argv[1]
    base_url = sys.argv[2]
    product_type = sys.argv[3]
    start = sys.argv[4]
    stop = sys.argv[5]
    area = sys.argv[6]

    execfile('.' + srcuser)

    dhusSearch = DhusSearch()
    result = dhusSearch.search(srcuser, srcpassword, base_url, product_type, start, stop, area)

    for line in result:
        print(line)

