#!/bin/bash

hdfs dfs -rm -r /output

hadoop jar target/department-partitioning.jar \
com.department.driver.DepartmentDriver \
/input /output
