from bs4 import BeautifulSoup
import math
import time
import pandas as pd
import urllib
import requests
import pymongo

client = pymongo.MongoClient('mongodb+srv://admin:admin@demo-1-4dbde.mongodb.net/test?retryWrites=true&w=majority')
db = client.jobstreet

def resolve_redirects(job_url):
  try:
    data = requests.request('get', job_url, headers={'User-Agent' : "Magic Browser"}).text
    return data
  except urllib.error.HTTPError as e:
    if e.code == 429:
      time.sleep(5)
      return resolve_redirects(job_url)
    raise

def data_finder(alist):
  res = [x for x in alist if x != None]
  res = res[0]
  res = res.text.strip().split('\n')[0]
  return res.strip()

def remove_blanks(alist):
  res = [x for x in alist if x != '']
  return res

def data_find(alist):
  res = [x for x in alist if x != None]
  res = res[0]
  res = res.text.strip().split('\n')
  return res

def get_first_page_data(keys):
  keywords = keys.replace(' ','+')
  url = 'https://www.jobstreet.com.sg/en/job-search/job-vacancy.php?key={}'.format(keywords)
  data = requests.request('get', url, headers={'User-Agent' : "Magic Browser"}).text
  soup = BeautifulSoup(data, 'html.parser')
  jobs_num = soup.find(lambda tag: tag.get('id') == 'job_count_range').text.split(' ')[4]
  if ',' in jobs_num:
    jobs_count = int(jobs_num.split(',')[0])*1000 + int(jobs_num.split(',')[1])
  else:
    jobs_count = int(jobs_num)
  jobs_count = math.ceil(jobs_count)
  return jobs_count

def get_all_job_links(keys, page_count):
  keywords = keys.replace(' ','+')
  job_links = []
  for i in range(int(page_count)):
    page = i+1
    print('blashf'+str(page))
    url = 'https://www.jobstreet.com.sg/en/job-search/job-vacancy.php?key={}&pg={}'.format(keywords, page)
    data = requests.request('get', url, headers={'User-Agent' : "Magic Browser"}).text
    soup = BeautifulSoup(data, 'html.parser')
    try:
      first = soup.find_all(lambda tag: tag.name == 'div' and tag.get('id') == 'job_listing_panel')[0]
      second = first.find_all(lambda tag: tag.name == 'a'and tag.get('class') == ['position-title-link'])
      for job in second:
        job_links.append(job.get('href'))
    except:
      pass
  job_links = list(filter(lambda a: a != 'https://www.jobstreet.com.sg/en/job/1', job_links))
  return job_links

def create_job_data(job_url):
  data = resolve_redirects(job_url)
  soup = BeautifulSoup(data, 'html.parser')
  first = [x for x in soup.find_all('div') if x.get('class')== ['panel', 'panel-clean']]
  if first != []:
    company_name = data_finder([x.find(lambda tag: tag.get('id') == 'company_name') for x in first])
    position_title = data_finder([x.find(lambda tag: tag.get('id') == 'position_title') for x in first])
    #print(company_name)
    try:
      years_of_experience = data_finder([x.find(lambda tag: tag.get('id') == 'years_of_experience') for x in first])
    except:
      years_of_experience = None
      pass

    try:
      if soup.find(lambda tag: tag.get('id') == 'single_work_location'):
        job_location = data_finder([x.find(lambda tag: tag.get('id') == 'single_work_location') for x in first])
      else:
        job_location = data_finder([x.find(lambda tag: tag.get('id') == 'multiple_work_location_list') for x in first])
    except:
      job_location = None
      pass


    job_desc = remove_blanks(data_find([x.find(lambda tag: tag.get('id') == 'job_description') for x in first]))
    job_desc = '.'.join(job_desc)

    try:
      address = data_finder([x.find(lambda tag: tag.get('id') == 'address') for x in first])
    except:
      address = None
      pass


    try:
      company_size = data_finder([x.find(lambda tag: tag.get('id') == 'company_size') for x in first])
    except:
      company_size = None
      pass
    try:
      company_industry = data_finder([x.find(lambda tag: tag.get('id') == 'company_industry') for x in first])
    except:
      company_industry = None
      pass
    try:
      avg_processing_time = data_finder([x.find(lambda tag: tag.get('class') == ['align-normal']) for x in first])
    except:
      avg_processing_time = None
      pass
    try:
      post_date = data_finder([x.find(lambda tag: tag.get('id') == 'posting_date') for x in first]).replace('Advertised: ','')
    except:
      post_date = None
      pass
    try:
      close_date = data_finder([x.find(lambda tag: tag.get('id') == 'closing_date') for x in first]).replace('Closing on ','')
    except:
      close_date = None
      pass
    try:
      ea_reg = data_finder([x.find(lambda tag: tag.get('id') == 'ea_registration_id') for x in first])
    except:
      ea_reg = None
      pass


    try:
      company_overview = remove_blanks([x.strip() for x in [x for x in first if x.find(lambda tag: tag.get('id') == 'company_overview')][0].text.split('\n')])
      company_overview = list(filter(lambda a: a not in ['COMPANY OVERVIEW', ' COMPANY OVERVIEW'], company_overview))[0]
    except:
      company_overview = ''

    dd = {'company_name' : company_name,
          'position_title' : position_title,
          'years_of_experience' : years_of_experience,
          'job_location' : job_location,
          'job_desc' : job_desc,
          'address' : address,
          'company_size' : company_size,
          'company_industry' : company_industry,
          'avg_processing_time' : avg_processing_time,
          'post_date' : post_date,
          'close_date' : close_date,
          'ea_reg' : ea_reg,
          'company_overview' : company_overview,
          'url' : job_url}
  else:
    return None
  return dd

def main_parser(keys):
  page = get_first_page_data(keys)
  job_links = get_all_job_links(keys, page)
  all_data = []
  for i in job_links:
    try:
      all_data.append(create_job_data(i))
    except:
      pass
  return all_data

def entry():
  all_jobs = main_parser('')
  all_jobs = [x for x in all_jobs if type(x) == dict and x != None]
  db.jobdata.insert_many(all_jobs)
  return "ok"
entry()