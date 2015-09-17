# ImportPipeline
This pipeline enables you to import data from various sources into ElasticSearch or other formats.
The main concepts of the pipeline are:
- Datasource<br/>
  A datasource is responsible for reading the foreign data. You can see this as a driver. 
- Pipeline 
- Endpoint

Other components are 
- PostProcessors
- Converters

## Logger
The ImportPipeline tries to log to Bitmanager's log system. This log-system can be downloaded from http://bitmanager.nl/distrib
 (Bitmanager.Core.Setup.exe).
## How to compile
The needed references are in the _references subdir. <br/>
Projects typically have a file 'assemblyinfo.template.cs' in their properties subdir. 
This template also contain also the auto-incremented version number of the current dll.<br/> 
Versions are incremented by 2 once the git-tree switches from clean to dirty (modified working tree).<br/>
Dirty builds have odd version numbers, clean builds have even version numbers.

The way this magic works it by having an executable registered as a customization in the MSBuild system.<br/>
The executable can be downloaded from http://bitmanager.nl/distrib (bmBuildHelper)<br/>
Installation is done by executing 'bmBuildhelper install'

