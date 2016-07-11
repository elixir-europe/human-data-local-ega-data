README

EgaDataService.jar 

EGA Database interaction server: internally provides most database functionality for the EGAPRO PostgreSQL database.

------------

Project is created using Netbeans.

Netbans uses ant, so it can be built using ant (version 1.8+). Ant target "package-for-store" creates the packaged Jar file containing all libraries. There are no further dependencies, everything is packaged in the Jar file.

Servers use Netty as framework. Client REST calls use Resty.

------------

