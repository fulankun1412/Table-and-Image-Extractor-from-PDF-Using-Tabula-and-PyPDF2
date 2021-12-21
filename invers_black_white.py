import numpy as np
import cv2 as cv
import os
from PIL import Image

image_dir = 'img_out'

for img in os.listdir(image_dir):
    if img.endswith('.png'):
        