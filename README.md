Modelling of data streams
=========

In this project we attempt to create a model for handling data streams based on fluid dynamics.

-----------------------------------------------------------------------------------------------------------
# I. Downloads and setup
-----------------------------------------------------------------------------------------------------------

1. Download Maven from: http://maven.apache.org/download.cgi  and unpack it in a directory of your choice (e.g. /usr/local/ ):

tar -xzf apache-maven-3.2.1-bin.tar.gz
 
2. Modify the file ~/.bashrc or ~/.bash_profile (the latter if you're on MacOSX), entering the paths of Maven and JVM installations (note - on Linux your JVM would typically be located under /usr/lib/jvm/* ):

export M2_HOME=/usr/local/apache-maven-3.2.1/
export M2=$M2_HOME/bin
export PATH=$M2:$PATH

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home
export PATH=$JAVA_HOME:$PATH

3. Clone the following git repository to your local file system:

    - via https:

        ```git clone https://github.com/przemek-g/streaming.git```

    - or via SSH:

        ```git clone git@github.com:przemek-g/streaming.git```

-----------------------------------------------------------------------------------------------------------
# II. Running
-----------------------------------------------------------------------------------------------------------

1. To verify the correctness of our setup, enter the streamProcessing project directory (the one where pom.xml is located) and run:

	```
	mvn test
	```

    Upon successful unit tests execution, you should see the 'BUILD SUCCESS' status message printed.

2. To run the basic topology application, type (while in the main project directory):

	```
	mvn compile exec:java -Dstorm.topology=fr.inria.streaming.examples.App 
	```

    (The topology will probably take a few seconds to perform its setup, then you should see lines of log messages. After the topology stops, you should see the 'BUILD SUCCESS' status message printed)

-----------------------------------------------------------------------------------------------------------
# III. How to use the API
-----------------------------------------------------------------------------------------------------------

Let's provide a quick overview of how to create a topology programmatically using the Storm library.

1. Prerequisites:

    A. *Topology* : a directed graph of distrbuted processing in Storm; 
   - nodes represent processing elements (they can receive portions of data, perform computational logic, and send the data further over some channel)), 
   - edges represent channels over which data is sent in a given direction

    B. *Spout* : a source of data stream in the topology (a node with only outgoing edges); the data it emits may come from different sources, e.g. it may be consumed from a message queue, read from database, or generated in any other way (in this example we read a text file and emit its lines)
    
    C. *Bolt* : a processing component of a topology, i.e. node that has incoming edges (and may or may not have outgoing edges). Bolts perform different stages of computational logic on our data streams. They can perform analysis of data, store data (e.g. database), etc. ...
    
    D. *Tuples* are portions of stream data; they contain key-value pairs (i.e. named fields with values)

2. Topology setup - running in 'local mode' (on a single machine) 

    (This is done in fr.inria.streaming.examples/App.java)

```
// For registering elements of our topology
TopologyBuilder builder = new TopologyBuilder(); 

// Register a spout (source of stream), i.e. instance of ISpout interface 
// The first param is an id for the component, the third one - number of threads that we want Storm to use for executing the logic of this component

builder.setSpout("text-spout", new TextContentSpout(fileName),1); 

// Register a bolt that performs some processing, i.e. instance of IBolt interface
// The first param is an id for the component, the third one - number of executor threads
// The method .shuffleGrouping("text-spout") creates a channel to stream data from 'text-spout' to this component and tells Storm that we want portions of stream to be distributed in a random yet load-balancing manner, to instances of this component 

builder.setBolt("splitter-bolt", new SentenceSplittingBolt(), 4).shuffleGrouping("text-spout"); 

// Analogous to the bolt definition above

builder.setBolt("stemmer-bolt", new WordStemmingBolt(),4).shuffleGrouping("splitter-bolt");

// Analogous to the bolt definitions above, the difference being the grouping of stream data.
// .fieldsGrouping means that portions of data (so called 'tuples') coming from 'stemmer-bolt' will be distributed among instances of this component according to a division based on a specific field ('docId'), i.e. tuples with docId='document_one' will go to the same instance of the bolt, tuples with docId='document_two' will all go to the same instance of bolt (however, we don't know whether 'document_one' tuples will go to the same bolt as 'document_two' tuples!)

builder.setBolt("index-bolt", new IndexingBolt(new TextFileIndexPersister("AppIndex.txt")),1).fieldsGrouping("stemmer-bolt", new Fields("docId"));

// Create the configuration for our topology

Config conf = new Config();
conf.setDebug(true);
conf.setMaxTaskParallelism(4); // sets an upper bound on the number of executor threads that can be created for a single component of our topology

// Submit the topology and its configuration to a LocalCluster representing the runtime for our topology on a local machine
LocalCluster cluster = new LocalCluster();
cluster.submitTopology("sample-streaming-topology", conf, builder.createTopology());

// Let the topology run for some time (milliseconds here)

Thread.sleep(4000);

// Close the topology
cluster.shutdown();
```
