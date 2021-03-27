# HanaDB-data-load
Script that loads dummy data into HanaDB for testing purposes.

This script was written to load dummy test data in a HANA database to
test different behaviorial aspects os a HA cluster.

Prerequisites
    1) SAP/Hana DB 2-node cluster (SLES4sap).  This script was developed and tested on 
       a AZURE sles12.sp4-sap 2-node perf-optimized cluster with SPS05.
       
    2) A SYSTEM DB user account (or equivalent). Although it is not required, you will not be able to use the CSV load feature.
