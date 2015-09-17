# ImportPipeline
This pipeline enables you to import data from various sources into ElasticSearch or other formats.
The main concepts of the pipeline are:
- Datasource<br/>
  A datasource is like a driver, and responsible for reading the foreign data. It converts the read data into a series of key-value pairs and send them to the pipeline. Some builtin datasources are:
  * TextDatasource
  * CsvDatasource
  * ESDatasource (for getting content from ElasticSearch)
  * SqlDatasource
  * JsonDatasource
  * XmlDatasource (preliminary...)
  * TikaDatasource (to import data from various documents)

- Pipeline <br/>
  The pipeline handles the key-value pairs from the datasource, and store them in fields, call scripts, convert the data, etc.
  The fields are hashes in a JObject that is provided by the associated endpoint.
- Endpoint<br/>An endpoint holds a current record that can be accessed by the pipeline. Once the record is finished (meaning: the pipeline calls an 'add' action), it will be 'added' to the dataset that the endpoint represents. Where a datasource is a driver for the input, the endpoint is a driver for the output. Some builtin endpoints are:
  * ESEndpoint (to put data in Elasticsearch)
  * JsonEndpoint
  * CsvEndpoint
  * TextEndpoint

Other components are 
- PostProcessors<br/>
  Postprocessors sit between a pipeline and an endpoint. Viewed from the pipeline, the post-processor is an endpoint. Viewed from an endpoint, the post-processor is a pipeline. Some builtin post-processors are:
  * SortProcessor
  * MapReduceProcessor (splitting, sorting, updupping, counting of data)<br/>
    This processor can work file or memory based.
- Converters

## Import driver
The importpipeline(dll) comes with a driver application: importer32 and importer64.<br/>
If this application is started without parameters, it shows a user interface from which you can start imports. If the application is started with at least the name of an import.xml, the application starts like a console application and does not show a user interface. 
## Logger
The ImportPipeline tries to log to Bitmanager's log system. This log-system can be downloaded from http://bitmanager.nl/distrib
 (Bitmanager.Core.Setup.exe).<br/>
The main logs are:
- error
- import
- import-debug
- import-missed

## How to compile
The needed references are in the _references subdir. <br/>
Projects typically have a file 'assemblyinfo.template.cs' in their properties subdir. 
This template also contain also the auto-incremented version number of the current dll.<br/> 
Versions are incremented by 2 once the git-tree switches from clean to dirty (modified working tree).<br/>
Dirty builds have odd version numbers, clean builds have even version numbers.

The way this magic works it by having an executable registered as a customization in the MSBuild system.<br/>
The executable can be downloaded from http://bitmanager.nl/distrib (bmBuildHelper)<br/>
Installation is done by executing 'bmBuildhelper install'

