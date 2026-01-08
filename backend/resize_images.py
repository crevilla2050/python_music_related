#!/usr/bin/env python3
"""
resize_images.py

Recursively crawls through a given directory (passed as a command line argument)
and resizes all found images to a maximum of 2500px in height or width,
preserving aspect ratio.

Usage:
    python resize_images.py /path/to/directory

Dependencies:
    - Pillow (PIL) library for image processing.
      Install with: pip install Pillow
"""

import sys
import os
from PIL import Image

# Supported image extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}

MAX_DIMENSION = 2500

def resize_image(image_path):
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            if width <= MAX_DIMENSION and height <= MAX_DIMENSION:
                # No resizing needed
                return False

            # Calculate new size preserving aspect ratio
            if width > height:
                new_width = MAX_DIMENSION
                new_height = int((MAX_DIMENSION / width) * height)
            else:
                new_height = MAX_DIMENSION
                new_width = int((MAX_DIMENSION / height) * width)

            resized_img = img.resize((new_width, new_height), Image.LANCZOS)
            resized_img.save(image_path)
            print(f"Resized: {image_path} from {width}x{height} to {new_width}x{new_height}")
            return True
    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return False

def crawl_and_resize(directory):
    # Initialize counters for total images and resized images
    total_images = 0
    resized_count = 0
    # Walk through the directory and its subdirectories
    for root, _, files in os.walk(directory):
        # Iterate through each file in the directory
        for file in files:
            # Get the file extension
            ext = os.path.splitext(file)[1].lower()
            # Check if the file extension is in the list of image extensions
            if ext in IMAGE_EXTENSIONS:
                # Increment the total images counter
                total_images += 1
                # Get the full path of the image
                image_path = os.path.join(root, file)
                # Resize the image and increment the resized images counter if successful
                if resize_image(image_path):
                    resized_count += 1
    # Print the total number of images processed and the number of images resized
    print(f"Processed {total_images} images, resized {resized_count} images.")

def main():
    if len(sys.argv) != 2:
        print("Usage: python resize_images.py /path/to/directory")
        sys.exit(1)

    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory.")
        sys.exit(1)

    crawl_and_resize(directory)

if __name__ == "__main__":
    main()
