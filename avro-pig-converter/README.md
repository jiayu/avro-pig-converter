# avro-pig-converter

## About the project

This was oringinally from a project I was working on with one of my client. We stored some data in avro format as metadata of the sequence file. When we read the metadata in Pig, we wanted to find a way to easily convert this metadata into tuple. As they were part of the sequence file, we could not use org.apache.pig.piggybank.storage.avro.AvroStorage from the piggybank. As a result, I started this project. 

For now, there is only one way conversion which is from Avro to Pig.

## TO-DOs

1. Add a sample project
2. Add Pig to Avro conversion

## Contact

If you find this interesting or you want to contribute, email me at jiayu.ji@gmail.com