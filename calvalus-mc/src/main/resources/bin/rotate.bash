#!/bin/bash
#
# Author: Peter Maloney
# Purpose: rotate input files so the oldest is overwritten if there are too many.
#     It is intended to be used by a backup script, and does not perform the actual backups.
#     If the source file is a .gz or .tgz, it is tested, otherwise only copied.

srcFilePath="$1"
destDir="$2"
copies="$3"

if [ "${srcFilePath}" = "" -o '!' -f "${srcFilePath}" ]; then
    echo "Missing or invalid srcFilePath: \"${srcFilePath}\""
    exit 1
fi
if [ "${destDir}" = "" ]; then
    echo "Missing destDir: \"${destDir}\""
    exit 2
fi
if [ '!' -d "${destDir}" ]; then
    if mkdir -p "${destDir}"; then
        true
    else
        echo "Missing or invalid destDir: \"${destDir}\""
        exit 3
    fi
fi
if [ "${copies}" = "" ]; then
    echo "Missing or invalid copies: \"${copies}\""
    exit 4
fi


# iso 8601 to the second, to enable natural sort order
timestamp=$(date --utc +%Y-%m-%dT%H:%M:%S)

srcFileName="$(basename "${srcFilePath}")"
srcFileBase=${srcFileName%.tgz}
destFileName="${timestamp}.${srcFileName}"

mv "${srcFilePath}" "${destDir}/${destFileName}"

countAll=0
countOk=0
countFailed=0
countDeleted=0

# Count files that are valid, and keep only the latest $copies of them
for file in $(ls -1tr "${destDir}/"*"${srcFileBase}.tgz" | sort -r); do
    if [ '!' -e "${file}" ]; then
        #added this because of strange behavior... sometimes file is absolute
        file="${destDir}/${file}"
    fi
    
    echo "Checking ${file}"
    
    if [ "${countOk}" -ge "${copies}" ]; then
        echo "    Deleting ${file}"
        rm "${file}"
        let countDeleted++
    else
        if tar tzf "${file}" >/dev/null 2>&1; then
            let countOk++
        else
            echo "    Corrupt ${file}"
            let countFailed++
        fi
    fi
    let countAll++
done

echo "countAll = ${countAll}"
echo "countOk = ${countOk}"
echo "countFailed = ${countFailed}"
echo "countDeleted = ${countDeleted}"
