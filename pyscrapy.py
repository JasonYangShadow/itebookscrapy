#!/usr/bin/python 
from mongo import Mongo
from exception import Type,TeleException
from config import Config
import urllib3
import logging
from bs4 import BeautifulSoup
from pprint import pprint
import datetime
import multiprocessing
import queue
from message import Message

website="http://www.allitebooks.com/"

LOGGER=logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('/tmp/pyscrapy.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
LOGGER.addHandler(fh)
LOGGER.addHandler(ch)
BUF_SIZE=10000
PROCESS_NUM=20

class PyScrapy:
	def __init__(self):
		self.__config=Config()
		self.__mongo=Mongo()
		self.__http=urllib3.PoolManager()
		self.__pyscrapy_config=self.__config.getValue('Config', 'DB_CONFIG')
		self.__pyscrapy_files=self.__config.getValue('Config', 'DB_FILES')
		self.__tele_msg=self.__config.getValue('Config', 'DB_MSG')
		self.__queue=queue.Queue(BUF_SIZE)

	def process(self):
		LOGGER.info('initialized...')
		r=self.__http.request('Get', website)
		if r.status==200:
			soup=BeautifulSoup(r.data.decode('utf-8','ignore'),"html.parser")
			for line in soup.find_all('span'):
				if line['class'][0]=='pages':
					pages=int(line.contents[0].split('/')[1].strip().split(' ')[0])
					record=self.__mongo.find(self.__pyscrapy_config)
					if len(record)>0:
						old_pages=record[0]['pages']
						if old_pages < pages:
							self.__mongo.saveUpdateOne({'pages':old_pages},{'$set':{'date':datetime.datetime.utcnow(),'pages':pages}},self.__pyscrapy_config)
							LOGGER.info('find new pages old:'+str(old_pages)+' new:'+str(pages))
							for num in range(int(pages-old_pages)):
								self.page_process("http://www.allitebooks.com/page/"+str(num+1)+"/")
					else:
						self.__mongo.insert([{'pages':pages,'date':datetime.datetime.utcnow()}], self.__pyscrapy_config)
						LOGGER.info('start processing '+str(pages)+' pages')
						for num in range(pages):
							self.page_process("http://www.allitebooks.com/page/"+str(num+1)+"/")
			while True:
				if self.__queue.empty():
					break
				else:
					article_url=self.__queue.get()	
					p=multiprocessing.Process(target=self.page_content_scrapy,args=(article_url,))
					p.start()
					p.join()
		else:
			LOGGER.error('request failed. '+website)

	def page_process(self,page_url):
		LOGGER.info('processing page:'+page_url)
		r=self.__http.request('Get',page_url)		
		if r.status==200:
			soup=BeautifulSoup(r.data.decode('utf-8','ignore'),"html.parser")
			for article in soup.find_all('article'):
				if article.has_attr("id") and article.has_attr("class"):
					if not self.__queue.full():
						self.__queue.put(article.div.a['href'])
					else:
						LOGGER.error('queue is full! please reset the buffer size')	
		else:
			LOGGER.error('request failed. '+page_url)

	def page_content_scrapy(self,article_url):
		LOGGER.info('processing article:'+article_url)
		r=self.__http.request('Get', article_url)
		if r.status==200:
			mongo=Mongo()
			soup=BeautifulSoup(r.data.decode('utf-8','ignore'),"html.parser")
			for head in soup.find_all('header'):
				if head.has_attr("class") and head['class'][0]=='entry-header':
					title=head.contents[1].string
					description=head.contents[3].string
			for div in soup.find_all('div'):
				if div.has_attr("class") and div['class'][0]=='entry-content':
					detail=div.get_text()
			for span in soup.find_all('span'):
				if span.has_attr("class") and span['class'][0]=='download-links' and span.a['href'].find('.pdf')!=-1:
					download_link=span.a['href']
					download_size=span.a.span.string
			mongo.saveUpdateOne({'title':title}, {'$set':{'description':description,'detail':detail,'download':download_link,'size':download_size,'article_url':article_url}},self.__pyscrapy_files)
			message=Message('New Books Update Notification', title, 'Description: '+description+" DownloadLink: <a href='"+download_link+"'>"+download_link+"</a>"+" ArticleLink: <a href='"+article_url+"'>"+article_url+"</a>"+" Size: "+download_size)
			mongo.insert([{'chat_user':'jasonyangshadow','chat_body':str(message)}], self.__tele_msg)
		else:
			LOGGER.error('request failed. '+article_url)

			
if __name__ == '__main__':
	scrapy=PyScrapy()
	scrapy.process()


		