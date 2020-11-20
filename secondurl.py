from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as soup

url = 'https://indianexpress.com/article/explained/explained-dissent-note-to-sonia-gandhi-by-23-senior-congress-leaders-6566752/'
req = Request(url, headers = {'User-Agent' : 'Mozilla/5.0'})
webpage = urlopen(req).read()
page_soup = soup(webpage, "lxml")
#data = page_soup.find_all('div', class_ = "sp-cn ins_storybody")
#print(data)
#print(page_soup.prettify()) 
content = page_soup\
   .find('div', class_ = 'full-details').getText()
   #.find_all('p')

#for i, elm in enumerate (content.childGenerator ()):
 #   print (i, ":", str (elm))
data = " ".join(content.split())
with open("2ndUrlData.txt",'w', encoding = 'utf-8') as f:
    f.write(data)
# to find a string 
# loc = str(Request.content).find('string name')