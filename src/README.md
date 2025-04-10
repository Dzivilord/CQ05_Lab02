# Lab 02: Advanced MapReduce & Spark Structured APIs

## Description
This homework consists of three main folders, each corresponding to one of the exercises related to Hadoop MapReduce and Apache Spark. Each folder contains the necessary source code, outputs, and supporting files for its respective task.

## Getting started

### Prerequisites
- Python > 3.10
- Spark : 3.5.5 
- Java 11+ (11 is recommended)
### Installation

Create and activate a virtual environment (optional but recommended)
```cmd
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```
Install dependencies
```cmd
pip install -r requirements.txt
```
## Project Structure 

```plain
<GroupID>
├── src
│   ├── Task_2.1
│   │   ├── 22120017.jar
│   │   ├── output.csv
│   │   └── source
│   │       ├── jar
│   │       │   └── SlidingWindowRevenue.jar
│   │       ├── build
│   │       │   ├── SlidingWindowMapper.class
│   │       │   ├── SlidingWindowReducer.class
│   │       │   └── SlidingWindowDriver.class
│   │       ├── SlidingWindowMapper.java
│   │       ├── SlidingWindowReducer.java
│   │       └── SlidingWindowDriver.java
│   ├── Task_2.2
│   │   ├── 22120017.ipynb
│   │   ├── output.csv
│   ├── Task_2.3
│   │   ├── 22120017.ipynb
│   │   ├── output.csv
│   ├── README               # Optional: instructions to run the code
│   └── requirements.txt     # Python dependencies
├── docs
│   └── Report.pdf
```
 