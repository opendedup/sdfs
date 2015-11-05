@echo off
bin\jre\bin\java -Dfile.encoding=UTF-8 -Djava.library.path=bin\ -classpath -XX:+UseG1GC -classpath lib\sdfs.jar;lib\* org.opendedup.sdfs.VolumeConfigWriter %*