# brite2caom2
Application to generate CAOM2 Observations from BRITE-Constellation files.

# How To Use brite2caom2

These are Linux-centric instructions.

In an empty directory (the 'working directory'), on a machine with Docker installed:

## Set Up

### Credentials

1. Run the following command to create a proxy certificate. You will be prompted for the password:

   ```
   docker run --rm -ti -v ${PWD}:/usr/src/app opencadc/brite2caom2 cadc-get-cert --cert-filename /usr/src/app/cadcproxy.pem --days-valid 10 -u <CADC User Name here>
   ```

The proxy certificate file will be valid for 10 days, and must be periodically renewed.

### File Location

`brite2caom2` will store files from local disk to CADC storage. This behaviour is controlled by configuration 
information. Most of the `config.yml` values are already set appropriately, but there are a few values that need to be 
set according to the execution environment. For a complete description of the `config.yml` content, see
https://github.com/opencadc/collection2caom2/wiki/config.yml.

1. Copy the file `config.yml` to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc/brite2caom2/master/config/config.yml
   ```

1. Tell `brite2caom2` in `config.yml` what to do with files on disk after the files have been stored to CADC:
   1. Set `cleanup_files_when_storing` to `True` or `False`. If this is set to `False`, `brite2caom2` will do nothing with the files on disk.
   If this is set to `True`, `brite2caom2` will move stored files to either a success or failure location.
   2. If `cleanup_files_when_storing` is set to `True`, set `cleanup_failure_destination`  and `cleanup_success_destination` to fully-qualified directory names that are visible within the Docker container. A directory is visible within a Docker container if it
   is one of the values on the right-hand-side of the colon in a `-v` `docker run` parameter.

1. Tell `brite2caom` in `config.yml` whether to re-submit duplicate files. 
   1. Set `store_modified_files_only` to `True` or `False`. If this is set to `False`, there is no effect on execution. If this is set to true, `brit2caom2`
   checks that the local version of the file has a md5 checksum that is different from the file at CADC before transferring the file to CADC storage. This affects only the `store` `task_types`.


## Initialize Execution Location (one time only)

1. In the master branch of this repository, find the scripts directory, and copy the files `brite_run.sh`  and `brite_run_incremental.sh` to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc/brite2caom2/master/scripts/brite_run.sh
   wget https://raw.github.com/opencadc/brite2caom2/master/scripts/brite_run_incremental.sh
   ```

1. Ensure the scripts are executable:

   ```
   chmod +x brite_run.sh
   chmod +x brite_run_incremental.sh
   ```

1. Edit the scripts to specify the file location:

   1. `brite_run.sh`:
      1. Find this line: `docker run --rm --name ${COLLECTION}_todo  --user $(id -u):$(id -g) -e HOME=/usr/src/app -v ${PWD}:/usr/src/app/ -v /data:/data ${IMAGE} ${COLLECTION}_run || exit $?`
      2. Replace the `/data/:` portion of the command with the fully-qualified directory name of where the application should find the data.

   1. `brite_run_incremental.sh`:
      1. Find this line: `docker run --rm --name ${COLLECTION}_state  --user $(id -u):$(id -g) -e HOME=/usr/src/app -v ${PWD}:/usr/src/app/ -v /data:/data ${IMAGE} ${COLLECTION}_run_incremental || exit $?`
      2. Replace the `/data/:` portion of the command with the fully-qualified directory name of where the application should find the data.

## Execution

`brite2caom2` may be run so that it processes files incrementally, according to their timestamp on disk, or so that is processes all the files it finds.

1. To run the application incrementally:

   ```
   ./brite_run_incremental.sh
   ```
   By default, incremental mode will start 24 hours prior to the current execution time. This can be changed by modifying the `state.yml` file content that is created on the first run.

1. To run the application on all the files it finds:

    ```
    ./brite_run.sh
    ```

## Debugging

1. To debug the application from inside the container, run the following command. Replace the `<data directory here>` with the fully-qualified path name of the directory where the data to be processed is located.

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v ${PWD}:/usr/src/app -v <data directory here>:/data --user $(id -u):$(id -g) -e HOME=/usr/src/app --name brite_run opencadc/brite2caom2 /bin/bash
   cadcops@53bef30d8af3:/usr/src/app# brite_run
   ```

1. For some instructions that might be helpful on using containers, see:
https://github.com/opencadc/collection2caom2/wiki/Docker-and-Collections

1. For some insight into what's happening, see: https://github.com/opencadc/collection2caom2

1. For Docker information, see: https://www.docker.com
