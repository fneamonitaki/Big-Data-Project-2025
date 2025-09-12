First open Apache Flink and start a cluster
./start-cluster.sh

Second activate venv 
source /home/user/flink-2.0.0/bin/venv/bin/activate


Command for running the simple queries 1-8
./flink run --python ~/my_job/my_job.py -pyExec /home/username/flink-2.0.0/bin/venv/bin/python

Command for running join queries 1-3
./flink run --python ~/my_job/my_job_join_synopses.py -pyExec /home/username/flink-2.0.0/bin/venv/bin/python
