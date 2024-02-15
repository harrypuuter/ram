#!/bin/bash

# first sleep for 30 seconds
sleep 30
# now generate a file with 1MB
dd if=/dev/zero of=/tmp/1MB bs=1024 count=1024
# now generate a yaml file with the test results
cat <<EOF > job_result.yaml
tests:
  - test: "test1"
    passed: True
    message: "test1 passed"
  - test: "test2"
    passed: True
    message: "test2 passed"
EOF

#
echo "Done"
exit 0

