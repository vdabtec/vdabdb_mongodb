# MongoDB Event Repository
### Overview 
This component implements a VDAB event repository using MongoDB.

### Features
<ul>
<li>Stores VDAB event data in the MongoDB database.
<li>Stores the payload in selectable formats including XML, JSON and Serialized.
<li>Allows retrieval and replay of historical events.
</ul>

### Licensing
Use of this software is subject to restrictions of the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).

This software makes us of the following components which include their own licensing restrictions:

| | | 
|  --- |  :---: |
| MongoDB Drivers - Apache| [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt) |

### Loading the the Package
The current or standard version can be loaded directly using the VDAB Android Client following the directions
for [Adding Packages](https://vdabtec.com/vdab/docs/VDABGUIDE_AddingPackages.pdf) 
and selecting the <i>AppDynNodes</i> package.
 
A custom version can be built using Gradle following the direction below.

* Clone or Download this project from Github.
* Open a command windows from the <i>MongoDB</i> directory.
* Build using Gradle: <pre>      gradle vdabPackage</pre>

This builds a package zip file which contains the components that need to be deployed. These can be deployed by 
manually unzipping these files as detailed in the [Server Updates](https://vdabtec.com/vdab/docs/VDABGUIDE_ServerUpdates.pdf) 
 documentation.
### Configuring VDAB
VDAB must be configured to work with this event database package. 
* Use the Android Client admin tool to Setup Event DB.
* Alternatively the container.xml file can be edited directly to define database.

### Known Issues as of 1 January 2018
* This code has not been widely tested.
* Queries have not been optimized.


