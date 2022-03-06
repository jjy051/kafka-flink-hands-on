# kafka
- apache-kafka install (through website)
- pip install kafka-python
- after installation, run ./bin/~.sh to run certain components
    - to run kafka cluster,
    - it needs zookeeper, kafka brokers, and kafdrop for monitoring
- docker-compose file is useful to run above several components automatically


# flink
- flink install (through website)
- pip install apache-flink (to install pyflink)
- in flink folder, there are several codes and configs
    - example codes, configuration
    - flink run scripts like spark-submit
        - basically it containes java (./bin/flink run examples/WordCount.jar)
        - it can works with python (./bin/flink --python examples/WordCount.py)

- cluster 로 실행하려면 managing 하는 component 를 실행시켜야 한다 (flink cluster)
    - ./bin/start-cluster.sh 등등
    - kafka 와 마찬가지로 docker-compose 파일 설정하여 자동화시킬 수 있음

- 내부적으로 manager - worker 로 구성되어 있기 때문에 python 파일 내에서 환경 설정을 해주어야 한다
    - spark session 과 비슷하게 python file 내에서 flink env 를 호출해야 한다

