Run Calvalus with SNAP (latest version)
=======================================

1. Create a SNAP-runtime environment in the Calvalus HDFS
    a. Create the directory:
        > sudo su - hadoop mkdir /calvalus/software/1.0/<snap-runtime-environment>
    b. Change the ownership of the directory:
        > sudo su - hadoop chown <USERNAME>:<USERGROUP> /calvalus/software/1.0/<snap-runtime-environment>
    c. Obtain the installer from the buildserver:
        > wget http://<username>:<password>@buildserver:8111/repository/download/senbox_Snap_installer/18816:id/installers/esa-snap_unix_<VERSION>.tar.gz
    d. Unpack (only the jars):
        > tar xzf esa-snap_unix_<VERSION>.tar.gz `tar tf esa-snap_unix_<VERSION>.tar.gz | grep jar`
    e. Copy the jars onto the HDFS (don't worry about 'Invalid argument' error messages):
        > for j in `find . -name "*.jar"`; do cp $j /calvalus/software/1.0/<snap-runtime-environment>; done


2. Build Calvalus with SNAP
    a. Checkout SNAP-Engine and SNAP-Desktop and build from sources (see [here](https://senbox.atlassian.net/wiki/display/SNAP/How+to+build+SNAP+from+sources) for a how-to)
    b. Important! Do a Maven install as well
    c. Check out Calvalus branch move_to_snap
    d. Maven build and install everything
    e. Create a calvalus runtime environment on the HDFS and set the ownership:
        > sudo su - hadoop mkdir /calvalus/software/1.0/<calvalus runtime environment>
        > sudo su - hadoop chown <USERNAME>:<USERGROUP> /calvalus/software/1.0/<calvalus-runtime-environment>
    f. Use the cpt.jar tool to deploy Calvalus to <calvalus-runtime-environment>

3. Use it!
    a. In your instance, copy cpt.jar and calvalus-production-X.X.jar created in step 2 to the lib directory
    b. Edit your my-script so that:
        # XXXXX_BEAM_VERSION points to <snap-runtime-environment> set up in step 1
        # XXXXX_CALVALUS_VERSION is the name of to the <calvalus-runtime-environment> set up in step 2