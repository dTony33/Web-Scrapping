from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as soup

url = "https://www.ndtv.com/india-news/details-of-dissent-letter-to-sonia-gandhi-steady-decline-no-honest-inspection-2286399"
req = Request(url, headers = {'User-Agent' : 'Mozilla/5.0'})
webpage = urlopen(req).read()
page_soup = soup(webpage, "lxml")
#data = page_soup.find_all('div', class_ = "sp-cn ins_storybody")
#print(data)
#print(page_soup.prettify()) 
content = page_soup\
   .find('div', {'itemprop': 'articleBody'}).getText()
   #.find_all('p')

#for i, elm in enumerate (content.childGenerator ()):
 #   print (i, ":", str (elm))
print(content.strip('\n'))
with open("1stUrlData.txt", 'w') as f:
   f.write(content)
# to find a string 
# loc = str(Request.content).find('string name')