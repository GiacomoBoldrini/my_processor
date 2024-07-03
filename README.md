# Intro

my_processor is a preliminary version of the new born ```spritz``` framework.
It is basically a bunch of independent scripts that should be run sequentially (with a little bit of massages) to process MC and data from nanoAOD directly to histograms.

# Image

A docker image is used to run the environment both on local machines or on condor jobs. This avoids mismatches in the various packages versions, avoids to ship tarballs or such to condor jobs and will, in future iterations, make the analysis reproducible out of the box.
The image should be pulled (~512 Mb) from 

https://gitlab.cern.ch/gpizzati/my_processor_image

```
# pulling the image .sif file
apptainer pull docker://gitlab-registry.cern.ch/gpizzati/my_processor_image:latest
# start a singularity shell with the image environment and condor support
apptainer shell -B /afs -B /cvmfs -B /eos -B ${XDG_RUNTIME_DIR}  --env KRB5CCNAME=${XDG_RUNTIME_DIR}/krb5cc my_processor_image_latest.sif
```


# Run the scripts and order

The flow is divided into N essential steps to be run sequentially
- ```fileset.py```: queries rucio to gather all possible files and replicas for a specific dataset that you want to analyze. Saves everything in json format
- ```chuncks.py```: takes the file list and number of events and subdivides it even further in chuncks of events to be shipped to condor or batch jobs.
- ```batch.py```: creates chunked json files for each job to analyze. Will create everything under condor/ and you can simply submit via ```condor_submit condor/submit.jdl```.
  Be sure that the following arguments are correctly set according to your filesystem in the condor submit job:
  
  ```
  use_x509userproxy = true
  should_transfer_files = YES
  transfer_input_files = $(Folder)/chunks_job.pkl, script_worker.py, ../data.tar.gz, ../framework.py, ../variation.py, ../modules
  MY.SingularityImage = "/eos/user/g/gboldrin/my_processor_image_latest.sif"
  ```

  Beware that the scripts will save outputs under ```condor_processor/results```. If you do not have this folder create it before launching jobs.
  
- ```merge_results.py```: Take pickled results from jobs (```hist``` histograms objects) and merges them into a single pickled file

Plotting is ongoing and currently done via jupy notebook to be shared privately.

# Processor

Everything about the analysis itself (cuts, objects, corrections and so on) is encoded in the ```script_worker.py``` script. It takes as input a nanoAOD and creates the final histograms so it is pretty convonlved at the time being but the usage of ```awkward``` arrays makes the syntax easily understandable.


