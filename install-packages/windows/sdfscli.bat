@echo off
bin\jre\bin\java -Djava.library.path=bin\ -classpath lib\sdfs.jar;lib\* org.opendedup.sdfs.mgmt.cli.SDFSCmdline %*