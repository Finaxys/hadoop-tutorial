#!/bin/bash
java -Xms512m -Xmx1024m -cp target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar:atom.jar:. fr.finaxys.tutorials.hadoop.AtomGenerate $*
