#!/usr/bin/env python3

import argparse
import os
import fnmatch

def find_files():
    matching_filters = ['pom.xml', 'HowToDeploy.txt', 'calvalus.properties', 'directory-listing.xsl',
                        'HadoopProcessingService.java', 'ProductionTool.java',
                        'calvalus.config', 'INSTALL.txt', 'ReportGeneratorTest.java' ]

    matching_filters = ['pom.xml', '*.java', '*.jsp', '*.txt', '*.properties','*.xsl','*.config']
    matches = []
    for root, dirnames, filenames in os.walk('.'):
        for filter in matching_filters:
            for filename in fnmatch.filter(filenames, filter):
                matches.append(os.path.join(root, filename))
    return matches

def read_files_litst():
    matches = []
    for line in open('updateVersionNumberFiles.cfg'):
        matches.append(line.strip())
    return matches

def update_verions_numbers():
    parser = argparse.ArgumentParser(description='Update version number in code.')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        required=False, help="Don't write any files, just pretend.")
    parser.add_argument('--not-exact', action='store_true', default=False,
                        required=False, help="Dont' be extremly exact with matching.")
    parser.add_argument('--find-files', action='store_true', default=False,
                        required=False, help="Searches for files containing the given old version number.")
    parser.add_argument('kind', choices=['calvalus', 'beam'])
    parser.add_argument('old_version', action='store')
    parser.add_argument('new_version', action='store')
    args = parser.parse_args()

    print (args)
    exact = not args.not_exact
    if args.find_files:
        filesToChange = find_files()
    else:
        filesToChange = read_files_litst()

    for file in filesToChange:
        print ()
        print (file)
        with open(file) as f:
            fileContent = f.readlines()
        changes = False
        changedLines = []
        fileContentNew = []
        for line in fileContent:
            if args.kind == 'calvalus':
                if 'version>' + args.old_version + '</' in line:
                    line = line.replace(args.old_version, args.new_version)
                    changes = True
                    changedLines.append(line)
                elif 'calvalus-' + args.old_version in line:
                    if exact:
                        if not 'calvalus-' + args.old_version + '-' in line:
                            line = line.replace(args.old_version, args.new_version)
                            changes = True
                            changedLines.append(line)
                    else:
                        line = line.replace(args.old_version, args.new_version)
                        changes = True
                        changedLines.append(line)
                elif 'ersion ' + args.old_version in line:
                    line = line.replace(args.old_version, args.new_version)
                    changes = True
                    changedLines.append(line)
            elif args.kind == 'beam':
                if 'beam-' + args.old_version in line:
                    if exact:
                        if not 'beam-' + args.old_version + '-' in line:
                            line = line.replace(args.old_version, args.new_version)
                            changes = True
                            changedLines.append(line)
                    else:
                        line = line.replace(args.old_version, args.new_version)
                        changes = True
                        changedLines.append(line)
            fileContentNew.append(line)
        if changes:
            if args.dry_run:
                for line in changedLines:
                    print (line, end='')
            else:
                fileHandle = open(file, 'w')
                fileHandle.writelines(fileContentNew)
                fileHandle.close()

if __name__ == '__main__':
    update_verions_numbers()