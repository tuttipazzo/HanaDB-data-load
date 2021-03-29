# HanaDB-data-load
Script that loads dummy data into HanaDB for testing purposes.

This script was written to load dummy test data in a HANA database to
test different behaviorial aspects of a HA cluster.

Prerequisites
1) SAP/Hana DB 2-node cluster (SLES4sap).  This script was developed and tested on 
   an AZURE sles12.sp4-sap 2-node perf-optimized cluster with SPS05.

2) Access to SAP HANA Database System Administrator account on each node.
3) A SYSTEM DB user account (or equivalent). Although it is not required, you will not be able to use the CSV load feature and will have to use the slower 1 record at a time function.
4) Know which node is primary.

Installation
On primary node:
1) sudo su - prdadm    
2) Copy src/*.py ~prdadm/

To Run

python populateHanaDB.py

