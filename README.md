# pyflink-example-application

<h1>Pyflink-example-application</h1>

<h2> Repository Structure </h2>

```
├── input
│   └── covid_19.csv
├── main.sh
├── output
│   └── covid_19.csv
└── scripts
    └── script.py
```


A simple PyFlink example demonstrating how to read files from a local system, perform basic transformations and aggregate functions,
using UDFs, and write it back as a csv file to another directory on a local system.


<h2> Running the code </h2>

Before you run the code, ensure the ```$FLINK_HOME``` environment variable is set to allow the bash script to 
access flink's bin folder and start the cluster. You will need to edit to bash script's ```python_script``` variable
to point to the location where the script resides on your local machine.

After this configuration is complete, simple run ./main.sh and check the Flink web UI. The output should be similar to this.

![alt-text] https://ci.apache.org/projects/flink/flink-docs-release-1.9/page/img/quickstart-setup/jobmanager-1.png


