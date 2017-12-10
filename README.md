# meteorite-analysis

A library that uses Hadoop to analyze a NASA dataset of meteorite landings.



## Dataset

The dataset can be found at this link:

https://data.nasa.gov/Space-Science/Meteorite-Landings/gh4g-9sfh

## Overview 
This project is to learn some cool things about meteorites, and to use Hadoop. Ultimately, this dataset is small enough that using a python stack would likely be quicker to develop and run, but Hadoop is a cool tool and I wanted to work with it.

## Build and Run
This project uses `maven` to manage its dependencies, and the build is geared towards a computer that already has Hadoop installed and running. Assuming that you have `maven`, run `mvn package` from the root directory. Then, different tasks can be run as follows:

`hadoop jar target/meteoriteanalysis-1.0-SNAPSHOT-job.jar com.nachtm.meteoriteanalysis.MAINCLASS data/meteorite_landings.csv output`

Right now, there are two jobs that can be run by replacing `MAINCLASS` with:

* `com.nachtm.meteoriteanalysis.MaxMass` outputs `1 x` where `x` is the mass, in grams, of the largest meteorite.

* `com.nachtm.meteoriteanalysis.NumByArea` outputs information about the density of meteorites. More specifically, each row defines a 5 degree by 5 degree box, and a count of the number of meteorites which have landed in that box. The format is `lat long count` where `lat` is the latitude of the center of the box, `long` is the longitude of the center of the box, and count is the number of meteorites which landed within the box.

## Reading the Output
Output is written to the `output` directory (or whatever filepath you give as the last argument to the run command). Assuming a successful run, the file that actually contains the output will be in `output/part-r-0000`. If there is enough output that multiple files are needed, look for `output/part-r-0001` and so on. 