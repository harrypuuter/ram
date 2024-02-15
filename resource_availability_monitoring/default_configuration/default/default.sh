#!/bin/bash

# first sleep for 30 seconds
sleep 30
# now generate a yaml file with the test results
cat <<EOF > job_result.yaml
tests:
  - test: "default_test"
    passed: True
    message: "default_test passed"
EOF
echo "Done"
exit 0

