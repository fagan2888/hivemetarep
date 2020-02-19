Hive Metastore Replication and Backup Tool

To Build:
Build tar/gz: mvn package -Ptar
Build rpm: mvn package -Prpm


Example to Run:
./hiveload.sh  -export -exportPath /user/hdfs/test -db processed_opendata_samples -useHdfs -krb_keytab /etc/security/keytabs/hdfs.headless.keytab -krb_upn hdfs-unit@UNIT.HDP.EXAMPLE.COM