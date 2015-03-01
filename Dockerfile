FROM centos
RUN yum update -y
RUN yum install -y java-1.7.0-openjdk
ADD ./build/libs/PetStoreLoadGenerator-1.0.jar /opt/PetStoreLoadGenerator-1.0.jar
ADD libs/bigpetstore-data-generator-0.2.jar /opt/bigpetstore-data-generator-0.2.jar
### Run w/ reasonable defaults, expected that a user will override.
CMD java -cp /opt/PetStoreLoadGenerator-1.0.jar:opt/* org.apache.bigtop.load.LoadGen
