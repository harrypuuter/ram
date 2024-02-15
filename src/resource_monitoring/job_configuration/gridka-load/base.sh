echo "Checking before running"
cat /etc/os-release
echo "Starting singularity build"
singularity build --sandbox execute docker://harrypuuter/load_test:cs7-v2
cd execute
ldd unittest_config_dyjets_2018
echo "Starting execution"
bash run.sh
cp job_result.yaml ../
echo "Done"
