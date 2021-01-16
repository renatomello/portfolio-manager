#%%
from sys import exit
from sys import stdout
from random import uniform
from time import sleep

from pandas import read_sql_query

from bs4 import BeautifulSoup as bs

from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from pytesseract import image_to_string
from PIL import Image, ImageOps
from base64 import b64decode
from argparse import ArgumentParser
import cv2

db_config = 'database.ini'
url = 'http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/'

options = ChromeOptions()
# options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-extensions")
options.add_argument("--proxy-server='direct://'")
options.add_argument("--proxy-bypass-list=*")
# options.add_argument("--start-maximized")
options.add_argument('--disable-gpu')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument('--ignore-certificate-errors')
options.add_argument('--no-sandbox')

#%%
driver = webdriver.Chrome(options = options)
driver.delete_all_cookies()

wait = WebDriverWait(driver, 15)

driver.get(url)
wait.until(
    EC.frame_to_be_available_and_switch_to_it("bvmf_iframe")
)
select = Select(driver.find_element_by_name('cboAno'))
select.select_by_index('2')

sleep(1.)

driver.switch_to_window(driver.window_handles[1])

sleep(1.)

image = driver.find_element_by_xpath('//img')

img_base64 = driver.execute_script("""
    var ele = arguments[0];
    var cnv = document.createElement('canvas');
    cnv.width = ele.width; cnv.height = ele.height;
    cnv.getContext('2d').drawImage(ele, 0, 0);
    return cnv.toDataURL('image/png').substring(22);    
    """, driver.find_element_by_xpath("//img"))
with open(r"image.png", 'wb') as f:
    f.write(b64decode(img_base64))

# img = Image.open('image.png')#.convert('RGB')
# img = ImageOps.autocontrast(img)
# gray = img.convert('L')
# gray.save('image_gray.jpg')
# bw = gray.point(lambda x: 0 if x < 1 else 255, '1')
# bw.save('image_trhd.jpg')
# image_to_string(gray)

#%%
#%%
driver.quit()