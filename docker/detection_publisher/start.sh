#!/bin/bash

# default values
INPUT_IMAGES_DEFAULT="input_images"
OUTPUT_IMAGES_DEFAULT="output_images"
RESULTS_DEFAULT="results"

if [[ -n $INPUT_IMAGES ]]; then
    input_images="$INPUT_IMAGES"
else
    input_images="$INPUT_IMAGES_DEFAULT"
fi

if [[ -n $OUTPUT_IMAGES ]]; then
    output_images="$OUTPUT_IMAGES"
else
    output_images="$OUTPUT_IMAGES_DEFAULT"
fi

if [[ -n $RESULTS ]]; then
    results="$RESULTS"
else
    results="$RESULTS_DEFAULT"
fi

echo "Will use following topics..."
echo "input_images: $input_images"
echo "output_images: $output_images"
echo "results: $results"

source /opt/ros/humble/setup.bash
exec python3 /root/detection_publisher.py input_images:=$input_images output_images:=$output_images results:=$results