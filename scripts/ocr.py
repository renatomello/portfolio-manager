# import the necessary packages
from PIL import Image
import pytesseract
import argparse
import cv2
import os
from numpy import float32

def deskew(image):
    (h, w) = image.shape[:2]
    moments = cv2.moments(image)
    skew = moments["mu11"] / moments["mu02"]
    M = float32([[1, skew, -0.5 * w * skew],[0, 1, 0]])
    image = cv2.warpAffine(image, M, (w, h), flags = cv2.WARP_INVERSE_MAP | cv2.INTER_LINEAR) 
    return image

# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--image", required=True, help="path to input image to be OCR'd")
# ap.add_argument("-p", "--preprocess", type = str, default = "thresh", help="type of preprocessing to be done")
args = vars(ap.parse_args())

# load the example image and convert it to grayscale
image = cv2.imread(args["image"])
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
# check to see if we should apply thresholding to preprocess the image
# if args["preprocess"] == "thresh":
#     gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
# # make a check to see if median blurring should be done to remove noise
# elif args["preprocess"] == "blur":
#     gray = cv2.medianBlur(gray, 3)
# write the grayscale image to disk as a temporary file so we can apply OCR to it
# filename = "{}.png".format(os.getpid())
gray_2 = Image.fromarray(deskew(gray))
gray_2.save('gray.png')
# gray_2.save('test.png')
cv2.imwrite('image_gray.png', gray)
bw = Image.fromarray(gray).point(lambda x: 0 if x < 233 else 255, '1')
bw.save('image_trhd.png')
# load the image as a PIL/Pillow image, apply OCR, and then delete the temporary file
text = pytesseract.image_to_string(Image.open('image_gray.png'))
# os.remove(filename)
print(text)
text_2 = pytesseract.image_to_string(Image.open('image_trhd.png'))
print(text_2)
# show the output images
# cv2.imshow("Image", image)
# cv2.imshow("Output", gray)
# cv2.waitKey(0)
