<!--
Copyright 2026 Brockmann Consult GmbH
-->

# Calvalus data cube generation

This document describes how to generate and update xcube data cubes on Calvalus.
The 2026 revision aims at replacing gen1-based workflows for cube extension.

## Introduction

### Purpose and scope

Purpose of this document is to describe processing requests, configurations, and processor packages to generate and
update xcube data cubes.

### Overview

If a time series is produced on a Calvalus processing system it is reasonable to convert it into a data cube on Calvalus
as well. Calvalus can make use of the fact that the xcube format is an open convention based on zarr with some
additional constraints. Calvalus uses a writer available as SNAP extension to directly write the zarr format required by
xcube.

The xcube format 

- first of all represents time series of multi-variable 2-D data on a common spatial grid, i.e. zarr variables with dimensions time, y, x or time, lat, lon
- has a convention for data pyramids that are represented by 6 levels of zarrs <name>.levels/0.zarr ... <name>.levels/5.zarr 
- has a convention for an additional time-chunked zarr often called <name>-t-opt.zarr suitable for time series extraction

A cube can be plain or leveled, and it optionally has a time-chunked cube aside.

There are two use cases for cube generation:

- backlog: generation of a data cube from a time series of data available on the same grid (else, run l3-agg first)
- nrt: extension of a data cube with new time steps, e.g. by daily Level 3 products

In the backlog case, Calvalus can write all chunks of a data cube concurrently and independent of each other. By providing each chunk file in its place in the zarr and by adding the metadata the cube is complete. For the plain cube and the leveled cube, each input is converted into one time-1 chunk. A final consolidation step consolidates the time variable itself that shall not be chunked.

Generating an additional time-chunked cube is organised as a map-reduce job where the inputs are read by a mapper each. The mappers stream the data to different reducers that collect the data for a chunk and write this chunk. All reading and all writing can happen concurrently.

In the extension use case, the consolidated time variable does no longer allow the isolated addition of one time step. Instead, 

- the cube skeleton needs to be read, 
- the data of a time step is added, 
- and the added data and the updated time variable and metadata are written to the cube storage. 

Extending the time chunked cube requires 

- to read metadata, time variable, and the last temporal chunk unless the new time step starts a new one. 
- The last chunk is uncompressed, extended by the new time step, and compressed again. 
- The updated data chunks, time and metadata are written to storage.

To make sure that an interrupted update of a cube does no harm to the cube irrespectively when the interrupt happens a careful partial backup of the updated data is required. An interrupt may have partially overwritten data and metadata, but not completely. The cube is in an inconsistent state. This state can be detected during the next update attempt and is automatically recovered from, if necessary repeatedly.
         
### Supplementary notes

The consolidation of the time variable is an essential step for xcube performance as time is read repeatedly, and a chunk size of 1 for the time variable itself results in long loading times.

To better support the daily nrt extension of data cubes we had suggested to add one more convention to xcube:

- A (plain) cube can be stored in two zarrs that are temporally adjacent. Up to a certain time the cube is time-consolidated. Later times are added to the second zarr with the more recent data that continues the cube. Daily additions are appended to the second zarr without the necessity to read anything. From time to time the cube is consolidated by moving data from one zarr to the other and consolidating the time variable of the first zarr.
- Similarily, the time-chunked cube can use the daily extension zarr to access recent time steps that are not yet part of the time-chunked cube. This saves computational expensive decompression and compression during extension every day. The time chunks are usually complete years.

There is a tool zappend that solves a similar problem as the Calvalus way of appending to a cube. It has a lock mechanism to avoid concurrent appends that would also prevent from appending to an inconsistent cube after an interrupt. At the time of development of the Calvalus way it was not clear whether recovery from an inconsistent state is fully automated.

The other reason zappend is not used is the massive parallel tranfer capabilities of s5cmd used in Calvalus. It speeds up skeleton retrieval and storage of backup data considerably. It has not been tested though whether zappend has the same speed.


### Document outline

After this introduction

- the reference part explains requests, processor desriptors, and processor packages for cube generation and extension
- the instruction part explains how to migrate gen1-based cube extension workflows to the approach described here

## Reference part

### Cube generation request

This use case assumes that there is a time series dataset with one product item per time step in the target grid with the variables that shall be converted into a cube. The time stamp is part of the metadata of each file such that SNAP is able to detect that time. It is stored at some location in a (distributed) file system accessible by Calvalus.

A preparatory step is to make an ordered list of the paths to the files of this dataset and assign a continuous frame number to each of the paths.

A request to convert this time series into a cube is specified as a JSON text file and may look like the following:

```json
{
    "productionName" : "OKOSAT MSI 32V cube",
    "processor"      : "cubegen-1.6/xcube-levels",
    "processorParameters" : {
        "parameters": {
            "timeAxisLength": 3,
            "chunkSizeX": 2048,
            "chunkSizeY": 2048,
            "namesNotToBeWritten": "num_obs,num_passes,centered_mjd,centered_max,lat,lon,y,x"
        }
    },
    "inputTableJson": { "entry": [
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/01/20250201-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 0, "writeChunksOnly": "false" },
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/02/20250202-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 1, "writeChunksOnly": "true" },
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/03/20250203-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 2, "writeChunksOnly": "true" }
    ]},
    "inputFormat"      : "NetCDF4-BEAM",
    "outputDir"        : "s3a://water-services/Norway",
    "outputReplacement": "test-okosat-s2-l3-32V-Norway-v2",
    "attempts"         : "1",
    "queue"            : "cyano"
}
```

The request specifies

- A name provided by the user. It will be displayed in queues.
- The processor, which is the combination of a processor package name and version and the processor (descriptor) name
- Processor parameters that override default values
- The inputs as a JSON-formatted table with columns input, timeIndex, and writeChunksOnly, in this case three product items
- An input format that determins the time from globa attributes product metadata
- The target directory as S3 location and basename of the <basename>.levels directory
- attempts and queue as examples of other parameters that can be overridden

The result will be available in s3://water-services/Norway/test-okosat-s2-l3-32V-Norway-v2.levels/ with sub-directories 0.zarr, ..., 5.zarr .

There is another processor to generate a cube without levels. Either the request above or the following one shall be used. The request is the same except for the processor:

```json
{
    "productionName" : "OKOSAT MSI 32V cube",
    "processor"      : "cubegen-1.6/xcube-zarr",
    "processorParameters" : {
        "parameters": {
            "timeAxisLength": 3,
            "chunkSizeX": 2048,
            "chunkSizeY": 2048,
            "namesNotToBeWritten": "num_obs,num_passes,centered_mjd,centered_max,lat,lon,y,x"
        }
    },
    "inputTableJson": { "entry": [
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/01/20250201-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 0, "writeChunksOnly": "false" },
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/02/20250202-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 1, "writeChunksOnly": "true" },
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/03/20250203-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 2, "writeChunksOnly": "true" }
    ]},
    "inputFormat"      : "NetCDF4-BEAM",
    "outputDir"        : "s3a://water-services/Norway",
    "outputReplacement": "test-okosat-s2-l3-32V-Norway-v2",
    "attempts"         : "1",
    "queue"            : "cyano"
}
```

The result will be available in s3://water-services/Norway/test-okosat-s2-l3-32V-Norway-v2.zarr/ .

### Time-chunked cube generation request

In addition to either a plain or a leveled cube a time-chunked cube can be generated. A time-chunked cube optimises time series access. The request looks like the following.

```json
{
    "productionName"   : "OKOSAT S2 32V t-opt cube",
    "processor"        : "cubegen-1.6/xcube-t-opt",
    "inputTableJson": { "entry": [
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/01/20250201-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 0, "writeChunksOnly": "false" },
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/02/20250202-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 1, "writeChunksOnly": "true" },
        { "input": "/calvalus/projects/okosat/l3-msi-0.7/32V/2025/02/03/20250203-P1D-L3-32V-60m-v0.7.nc", "timeIndex": 2, "writeChunksOnly": "true" }
    ]},
    "inputFormat"      : "NetCDF4-BEAM",
    "aggregationParameters" : {
        "parameters": {
            "aggregators": {
                "aggregator": [ {
                    "varNames": "CHL,CHL_algo,Turbidity,Turbidity_algo,SDD,adg",
                    "timeShape": 3,
                    "yShape": 11640,
                    "xShape": 7780,
                    "timeChunks": 30,
                    "yChunks": 256,
                    "xChunks": 256,
                    "cubeMetadata": "{\"project\":\"okosat\",\"creator\":\"Brockmann Consult GmbH\"}"
                } ]
            }
        }
    },
    "outputDir"        : "s3a://water-services/Norway/test-okosat-s2-l3-32V-Norway-v2-t_opt.zarr",
    "queue"            : "cyano",
    "attempts"         : "1",
    "maxReducers"      : "8"
}
```

- The processor is cubegen-1.6/xcube-t-opt.
- The input table and the input format is the same as with the previous requests to create the main cube.
- The variables to be included in the cube are listed. The RGB channels are usually dropped here.
- The extend of the cube as well as the chunking is defined.
- Optionally, cube metadata can be set as.
- The output directory by convention is close to the main cube.
- Other parameters like queue, attempts can be overridden.
- Reducing maxReducers from the default 16 to 8 is reasonable for this test as there is a single time step only. The number of reducers needs to be large enought not to overload a single reducer to collect its part of the output.
      
### Credentials

TBD

### Understanding cube generation: descriptor and processors

There are three processor descriptors that corresond to the three example requests of the previous section. They use processors and writers of zarr for SNAP. 

The descriptor for cubegen-1.6/xcube-levels looks like the following:

```json
{
    "processorType"    : "snap-operator",
    "productionType"   : "processing",

    "processorName"    : "XCube",
    "processorParameters" : {
        "parameters": {
            "chunkSizeX": 2048,
            "chunkSizeY": 2048,
            "namesNotToBeWritten": "num_obs,num_passes,centered_mjd,centered_max,lat,lon,y,x",
            "resampleOnPyramidLevels": true,
            "encoding": "littleendian",
            "jsonFormattedCubeMetadataStr": "{}"
        }
    },

    "checkIntersection": "false",
    "inputSubsetting"  : "false",
    "outputSubsetting" : "true",

    "outputFormat"     : "xcube-levels",
    "outputPattern"    : ".*",

    "queue"            : "default",
    "failurePercent"   : "0",
    "attempts"         : "3",
    "timeout"          : "3600",
    "forceReprocess"   : "true",
    "processingMemory" : "7680",
    "snap.tileCache"   : "3072",
    
    "mapreduce.map.cpu.vcores" : "4",

    "processorBundles" : "cubegen-1.6",
    "calvalus"         : "calvalus-2.29",
    "snap"             : "snap-9.7cv",

    "processorDescriptor": {
        "title": "xcube levels generation",
        "version": "1.0",
        "description": "Initial concurrent generation of an xcube with a pyramid",
        "authorisation": [ "group:bc" ],
        "inputProductTypes": [ "L3" ],
        "outputProductType": "xcube-levels",
        "outputFormats": [ "zarr" ],
        "parameterDescriptor": {
            "queue": {
                "title": "Processing queue, default 'hroc4'",
                "type": "string"
            },
            "attempts": {
                "title": "Number of retries, default '4'",
                "type": "string"
            }
        }
    }
}
```

The descriptor complements the parameters provided in the request. It contains

- processor name XCube with default parameters
- parameter resampleOnPyramidLevels and output format xcube-levels to generate a levels cube
- memory settings and other default settings
- number of vcores per task, 4 for a cluster in the cloud that is configured with a factor of 4 vcores per available vcore
- processor package
- descriptor with authorisation for group bc
    
The processor package cubegen-1.6 contains the following files:

```text
  /calvalus-production2:/cyanoalert/calvalus/software/1.0/cubegen-1.6:
  total used in directory 4540 available 4 GiB       
  -rwxrwxr-x 1 cvop    1744 Nov 21 12:20 xcube-levels-descriptor.json*
  -rwxrwxr-x 1 cvop    1744 Nov 21 12:20 xcube-zarr-descriptor.json*   
  -rwxrwxr-x 1 cvop    2182 Nov 21 11:58 xcube-t-opt-descriptor.json*
  -rwxrwxr-x 1 cvop  359548 Apr 13  2026 snap-cubes-1.1.6-SNAPSHOT.jar*
  -rwxrwxr-x 1 cvop   78349 Apr 13  2026 jzarr-0.3.7-SNAPSHOT.jar*
  -rwxrwxr-x 1 cvop 4061482 Apr 13  2026 cdm-core-5.3.3.jar*
  -rwxrwxr-x 1 cvop   41123 Apr 13  2026 commons-cli-1.2.jar*
  -rwxrwxr-x 1 cvop   11008 Apr 13  2026 jblosc-1.0.1.jar*
  -rwxrwxr-x 1 cvop   53352 Apr 13  2026 libblosc.so*
```
 
- The first three files are the descriptors explained here.
- The jar files and the shared library contain the implementation of the XCube SNAP operator and the writer for the xcube-levels and xcube-zarr format.
    
The descriptor for time-chunked cube generation is a bit different because the input products do not match the chunks being written. Several temporally subsequent input products contribute to a temporal chunk, and each input product contributes to several spatial chunks. This is a typical case for map-reduce. A CubeMapper, CubePartitioner, and CubeReducer of Calvalus implement this production type. The descriptor looks like the following:

```json
{
    "productionType"   : "l3-agg",

    "mapreduce.job.map.class" : "com.bc.calvalus.processing.mosaic2.CubeMapper",
    "mapreduce.job.partitioner.class" : "com.bc.calvalus.processing.mosaic2.CubePartitioner",
    "mapreduce.job.reduce.class" : "com.bc.calvalus.processing.mosaic2.CubeReducer",
    "mapreduce.map.output.key.class" : "com.bc.calvalus.processing.mosaic2.CubeIndexWritable",
    "mapreduce.map.output.value.class" : "com.bc.calvalus.processing.mosaic2.CubeChunkWritable",
    
    "checkIntersection": "false",

    "aggregationParameters" : {
        "parameters": {
            "aggregators": {
                "aggregator": [ {
                    "type": "TimeChunkedCube",
                    "yChunks": 256,
                    "xChunks": 256,
                    "byteOrder": "littleendian",
                    "compression": "zlib,level:1"
                } ]
            }
        }
    },
    
    "mapreduce.job.outputformat.class" : "org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat",
    "mapreduce.output.lazyoutputformat.outputformat" : "com.bc.calvalus.processing.beam.S3aCompatibleOutputFormat",

    "queue"            : "default",
    "failurePercent"   : "0",
    "attempts"         : "3",
    "timeout"          : "3600",
    "forceReprocess"   : "true",
    "processingMemory" : "4096",
    "aggregationMemory": "4096",
    "maxReducers"      : "16",
    
    "mapreduce.map.cpu.vcores" : "4",
    "mapreduce.reduce.cpu.vcores" : "4",

    "calvalus"         : "calvalus-2.29",
    "snap"             : "snap-9.7cv",

    "processorDescriptor": {
        "title": "xcube t-opt generation",
        "version": "1.0",
        "description": "Initial concurrent generation of a time-chunked xcube",
        "authorisation": [ "group:bc" ],
        "inputProductTypes": [ "L3" ],
        "outputProductType": "xcube-t-opt",
        "outputFormats": [ "zarr" ],
        "parameterDescriptor": {
            "queue": {
                "title": "Processing queue, default 'default'",
                "type": "string"
            },
            "attempts": {
                "title": "Number of retries, default '3'",
                "type": "string"
            }
        }
    }
}
```

- The production type is based on l3-agg but defines its specific mapper and reducer implementations. They are part of calvalus, i.e. not part of the processor package.
- The defaults for aggregation parameters provide spatial chunks of 256 x 256 pixels.
- memory settings and other default settings
- number of vcores per task, 4 for a cluster in the cloud that is configured with a factor of 4 vcores per available vcore
- no processor package, but calvalus and snap
- descriptor with authorisation for group bc


### Cube extension requests


### Cube extension explained: descriptors and processors


### Instruction part
  
## Differences between gen1-based cubes and SNAP-writer-based cubes

## How to convert a gen1-based processing chain

## How to configure generation of a new cube



