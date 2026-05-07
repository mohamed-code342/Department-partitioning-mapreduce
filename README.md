# Department-Based Partitioning (Hadoop MapReduce)

## 📌 Overview

This project demonstrates custom partitioning in Hadoop MapReduce by distributing employee data across reducers based on department.

## 🎯 Objective

* Process employee salary data
* Assign each department to a dedicated reducer
* Compute:

  * Total Salary
  * Average Salary
  * Employee Count

## 🏗️ Architecture

* Mapper: extracts department and salary
* Custom Partitioner: assigns department → reducer
* Reducer: performs aggregation

## ⚙️ Tech Stack

* Java
* Hadoop MapReduce
* Maven

## 🔥 Key Feature

Custom Partitioner ensures:

* One reducer per department
* Efficient parallel processing

## 📂 Project Structure

* mapper/
* reducer/
* partitioner/
* driver/

## 🧪 Sample Input

E001,IT,5000,Developer,2020-01-15

## 📤 Sample Output

IT    Total: 15800   Avg: 5266   Employees: 3

## ⚠️ Edge Cases

* Invalid salary values are skipped
* Missing fields handled safely

## 🚀 How to Run

```bash
mvn clean package
hadoop jar target/department.jar com.department.driver.DepartmentDriver input output
```

## 📈 Notes

* Custom partitioning improves scalability
* Reducers work independently per department
