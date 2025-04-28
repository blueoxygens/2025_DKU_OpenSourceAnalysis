#!/bin/bash

# Array of read/write sizes to test
sizes=(10000 1000000)

# Loop through each size
for size in "${sizes[@]}"; do
    echo "Testing with read/write size: $size"
    
    # Loop through options 0 to 6
    for option in {0..6}; do
        echo "Running with option: $option"
        
        # Run the program with a timeout of 60 seconds
        timeout 60s ./lab2_bplustree $size $size $option
        
        # Check the exit status of the timeout command
        if [ $? -eq 124 ]; then
            echo "Test with size $size and option $option timed out after 60 seconds. Moving to next test."
        else
            echo "Test with size $size and option $option completed."
        fi
    done
done

echo "All tests completed."