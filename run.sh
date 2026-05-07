#!/bin/bash

hadoop jar Jar/departmentpartitioning.jar departmentpartitioning.DepartmentDriver \
-Dyarn.app.mapreduce.am.staging-dir=/user/cloudera/tmp \
-Dmapreduce.job.working.dir=/user/cloudera/tmp \
/user/cloudera/input/department/employees_sample.txt \
/user/cloudera/DepartmentDriver/result_sample
