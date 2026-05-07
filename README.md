#🧠 Department-Based Partitioning – Big Data MapReduce Project

## 📌 Project Description

This project implements a scalable **Hadoop MapReduce solution** for analyzing employee records across multiple departments. The system processes structured employee datasets to calculate:

* **Total salary per department**
* **Average salary per department**
* **Number of employees per department**

The project demonstrates distributed data processing using **Apache Hadoop**, with optimizations through a **Custom Partitioner** and **Combiner** to improve performance and reduce network overhead. It is designed to handle large datasets efficiently while maintaining robust data validation and fault tolerance.

---

## 🎯 Objectives

* Process large-scale employee datasets using distributed computing
* Partition data so each reducer handles a specific department
* Optimize shuffle performance using a Combiner
* Handle malformed, missing, or invalid salary records
* Produce accurate salary statistics for each department
* Demonstrate real-world Big Data engineering practices

---

## ⚙️ Tech Stack

| Technology                 | Purpose                               |
| -------------------------- | ------------------------------------- |
| 🟡 Apache Hadoop           | Distributed data processing           |
| ☕ Java                     | Core MapReduce implementation         |
| 💾 HDFS                    | Distributed storage                   |
| ⚡ MapReduce                | Batch data processing                 |
| 🧩 Custom Partitioner      | Department-based reducer distribution |
| 🔄 Combiner                | Local aggregation optimization        |
| 🖥️ Cloudera QuickStart VM | Development environment               |
| 🧪 Git & GitHub            | Version control                       |

---

## 🏗️ System Architecture

```text
Employee Dataset (CSV / TXT)
          │
          ▼
     EmployeeMapper
(Extract department & salary)
          │
          ▼
 DepartmentPartitioner
(Route records by department)
          │
          ▼
    SalaryCombiner
(Local partial aggregation)
          │
          ▼
   Hadoop Shuffle & Sort
          │
          ▼
     SalaryReducer
(Final salary statistics)
          │
          ▼
      Output Results
(Total, Avg, Employee Count)
```

---

## 🚀 Key Features

* 📊 Department-wise salary analysis
* ⚡ Optimized network performance using Combiner
* 🎯 Custom reducer allocation per department
* 🛡️ Robust error handling:

  * Missing salary values
  * Invalid numeric formats
  * Negative salaries
  * Malformed records
* 📈 Scalable for datasets 1GB+
* 🔄 Supports multiple reducers
* 🧠 Hadoop counters for data-quality monitoring
* 📂 Sample + real dataset support

---

## 📂 Project Structure

```text
DepartmentPartitioning/
│
├── src/
│   └── departmentpartitioning/
│       ├── DepartmentDriver.java
│       ├── DepartmentPartitioner.java
│       ├── EmployeeMapper.java
│       ├── SalaryCombiner.java
│       └── SalaryReducer.java
│
├── input_data/
│   └── employees_sample.txt
│
├── outputData/
│   └── result_sample.txt
│
├── Jar/
│   └── departmentpartitioning.jar
│
├── README.md
└── pom.xml
```

---

## 🛠️ Setup & Installation

### 1️⃣ Clone the repository

```bash
git clone https://github.com/mohamed-code342/Department-partitioning-mapreduce
cd Department-partitioning-mapreduce
```

### 2️⃣ Compile source files

```bash
javac -classpath `hadoop classpath` -d . src/departmentpartitioning/*.java
```

### 3️⃣ Build JAR

```bash
jar -cvf departmentpartitioning.jar -C . .
```

### 4️⃣ Upload dataset to HDFS

```bash
hdfs dfs -mkdir -p /user/cloudera/input/department
hdfs dfs -put input_data/employees_sample.txt /user/cloudera/input/department/
```

### 5️⃣ Run the project

```bash
hadoop jar departmentpartitioning.jar departmentpartitioning.DepartmentDriver \
/user/cloudera/input/department/employees_sample.txt \
/user/cloudera/output/result_sample
```

### 6️⃣ View results

```bash
hdfs dfs -cat /user/cloudera/output/result_sample/part-r-*
```

---

## 📌 Sample Input

```text
E001,IT,5000,Developer,2020-01-15
E002,IT,6000,Senior Developer,2019-03-20
E003,HR,4000,HR Manager,2021-02-10
E004,Sales,4500,Sales Executive,2020-05-15
E005,Sales,5500,Sales Manager,2019-08-20
E006,Finance,5200,Accountant,2020-11-10
E007,IT,4800,Junior Developer,2022-01-05
E008,HR,3800,HR Assistant,2021-06-15
```

---

## 📌 Expected Output

```text
IT        Total: 15800   Avg: 5266   Employees: 3
HR        Total: 7800    Avg: 3900   Employees: 2
Sales     Total: 10000   Avg: 5000   Employees: 2
Finance   Total: 5200    Avg: 5200   Employees: 1
```

---

## 🧪 Performance Optimization

### Without Combiner:

* Higher shuffle traffic
* Increased network overhead
* Slower reducer performance

### With Combiner:

* Reduced intermediate records
* Lower network usage
* Faster execution time
* Better scalability

---

## 🔮 Future Enhancements

* 🚀 Apache Spark migration
* 📊 Dashboard visualization with Dash / Power BI
* 🔄 Kafka streaming integration
* ☁️ Cloud deployment (AWS EMR / Azure)
* 🧠 Machine learning salary forecasting
* 📂 Support for Parquet/Avro formats
* 🔐 Security enhancements

