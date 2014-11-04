#!/usr/bin/env python

from string import join, split, strip
from sys import exit, argv, stderr
from pyhdf.SD import SD, SDC

class MiniODLParser:

    def parseFromString(self, str, vals):
        stringlines = str.split('\n')
        return self.parse(stringlines, vals)
    
    def parse(self, list, vals):
        """
        This function returns all or only requested header values.
        To get all, call this function with vals=['all'].
          (Note that vals must be a list.)
        Where objects are declared in the header, these are keyed with
          objectName:key, so to get the MEAN for the IMAGE object, call with
          vals=['IMAGE:MEAN'], and then the image mean will equal
          float(imgObj.values['IMAGE:MEAN']).
        A list of objects is passed as imgObj.values['objects'], and the
          original order of the header values as imgObj.values['keylist'].
        """
        if vals in [[],[None],[''],['none']]: return {} #empty dictionary
        dict = {'objects':[],'keylist':[]}  #keylist = original order of keys
        obj = ''
        key = ''
        for s in list:
            if s.find('=') == -1:           #not a line of key = data
                if s == '/': continue    #it's a comment
                if s == 'END': break        #it's the end
                if key:
                    if dict[key][-1] in ['"',' ']:
                        dict[key] += s      #concatenate run-on string
                    else: dict[key] += ' ' + s
                    continue
                
                # silently fail
                #print 'pds:pdsHeaderVals error: don\'t know what to do with'
                #print '\t',s
                continue
            t = split(s,'=')
            t[0] = strip(t[0])
            t[1] = strip(t[1])
            if t[0] == 'OBJECT':            #begin object values
                obj = t[1]
                key = ''
                dict['objects'].append(obj)
                continue
            if t[0] == 'END_OBJECT':        #end object values
                obj = ''
                key = ''
                continue
            if obj: key = obj + ':' + t[0]
            else: key = t[0]
            dict['keylist'].append(key)
            dict[key] = self.__clean(t[1])
        retval ={'objects':dict['objects'],'keylist':[]}
        for key in dict['keylist']:
            if vals == ['all'] or key in vals:
                retval['keylist'].append(key)
                retval[key] = dict[key]
        #--- end debug
        return retval
        #end def pdsHeaderValues
        
    def __clean(self, str):
        return str[1:len(str)-1]

#################################################################
def get_flip(filename):
    hdf = SD(filename, SDC.READ)
    global_attributes = hdf.attributes()
    
    if 'CoreMetadata.0' in global_attributes:
        # modis l1b 
        coreMetaData = global_attributes['CoreMetadata.0']
        odlParser = MiniODLParser()
        coreMetaDict = odlParser.parseFromString(coreMetaData, ['all'])
  
        platform = coreMetaDict['ASSOCIATEDPLATFORMSHORTNAME:VALUE']
        dnflag = coreMetaDict['DAYNIGHTFLAG:VALUE']

        print >> stderr,"platform ", platform
        print >> stderr,"dnflag ", dnflag
  
        if platform == 'Aqua' and dnflag == 'Day':
            print 'flipXY'
        elif platform == 'Terra' and dnflag == 'Night':
            print 'flipXY'
    else:
        startNode = global_attributes['Start Node']
        print >> stderr, 'start node', startNode
        endNode = global_attributes['End Node']
        print >> stderr, 'end node', endNode
    
        if startNode == 'Ascending' and endNode == 'Ascending':
            print 'flipXY'
        

if __name__ == '__main__':
    get_flip(argv[1])
