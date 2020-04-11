#!/bin/bash

SQL_DB_HOME_DIR=/home/af/Documents/PersonalProjects/ctennisdb/

SQL_TABLES=${SQL_DB_HOME_DIR}tables/*.sql
SQL_FOREIGN_KEYS=${SQL_DB_HOME_DIR}foreign_keys/*.sql
SQL_INDEXES=${SQL_DB_HOME_DIR}indexes/*.sql
SQL_FUNCTIONS=${SQL_DB_HOME_DIR}functions/*.sql
SQL_VIEWS=${SQL_DB_HOME_DIR}views/*.sql
SQL_PROCEDURES=${SQL_DB_HOME_DIR}stored_procedures/*.sql
SQL_DATA_NON_DEPENDENT=${SQL_DB_HOME_DIR}tables/data/non_dependent/*.sql
SQL_DATA_DEPENDENT=${SQL_DB_HOME_DIR}tables/data/dependent/*.sql

SQL_DATA_BASE_NAME=ctennisdbdev
#CMD_FOR_SQL="sudo -u postgres psql -d ${SQL_DATA_BASE_NAME}"

echo "Running the tables scripts ...:"
for file in $SQL_TABLES
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the tables scripts ...Done"


echo
echo "Running the foreign keys scripts ...:"
for file in $SQL_FOREIGN_KEYS
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the foreign keys scripts ...Done"


echo
echo "Running the indexes scripts ...:"
for file in $SQL_INDEXES
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the indexes scripts ...Done"


echo
echo "Running the functions scripts ...:"
for file in $SQL_FUNCTIONS
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the functions scripts ...Done"


echo
echo "Running the views scripts ...:"
for file in $SQL_VIEWS
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the views scripts ...Done"


echo
echo "Running the stored procedures scripts ...:"
for file in $SQL_PROCEDURES
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the stored procedures scripts ...Done"


echo
echo "Running the data scripts ...:"
for file in $SQL_DATA_NON_DEPENDENT
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done

for file in $SQL_DATA_DEPENDENT
do
	echo + $(basename $file) # echo $file to display the full name of the file
	sudo -u postgres psql -d ${SQL_DATA_BASE_NAME} -f ${file}
done
echo "Running the data scripts ...Done"

exit