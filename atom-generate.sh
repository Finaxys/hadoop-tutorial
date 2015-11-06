#!/bin/bash
java -cp target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar:atom.jar:. fr.finaxys.tutorials.hadoop.AtomGenerate $*
