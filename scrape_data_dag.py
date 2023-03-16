from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_data_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def scrape_data():
    # Import necessary libraries
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    import time

    # List of URLs to scrape
    mainUrl = 'https://www.bbc.co.uk'
    response = requests.get(mainUrl+'/news/coronavirus' )
    soup = BeautifulSoup(response.content, 'html.parser')
    newsPage = soup.find('span', {'class' : 'lx-pagination__page-number qa-pagination-current-page-number'})
    print(newsPage.text)
    while int(newsPage.text) >= 1 :
        dataUrls = soup.find_all('a', {'class': ['gs-c-promo-heading gs-o-faux-block-link__overlay-link gel-pica-bold nw-o-link-split__anchor','gs-c-promo-heading gs-o-faux-block-link__overlay-link gel-paragon-bold gs-u-mt+ nw-o-link-split__anchor','qa-heading-link lx-stream-post__header-link']})
        dataUrls = list(dict.fromkeys(dataUrls))
        df = pd.DataFrame()
        body = ""
        df['title'] = []
        df['date'] = []
        df['subtitle'] = []
        df['topic'] = []
        df['text'] = []
        df['image'] = []
        df['authors'] = []
        p=0
        for dataUrl in dataUrls:
            #time.sleep(6)
            url = dataUrl['href']
            if "bbc.com" in url :
                break 
            url = mainUrl+url
            #print(url)

            dataResponse =  requests.get(url)
            soupArticle = BeautifulSoup(dataResponse.content, 'html.parser')
            titleElement = soupArticle.find('h1', {'id': 'main-heading'})
            if titleElement == None : 
                title = ""
            else:
                title= titleElement.text 

            date = soupArticle.find('time', {'data-testid': 'timestamp'})['datetime']
            date = datetime.strptime(date, '%Y-%m-%d')
            subtitleElement = soupArticle.find('b', {'class': 'ssrcss-hmf8ql-BoldText'})
            if subtitleElement == None : 
                subtitle = ""
            else:
                subtitle= subtitleElement.text 
            topicElement = soupArticle.find('a', {'class' : 'ssrcss-w6az1r-StyledLink ed0g1kj0'})
            if topicElement == None : 
                topic = ""
            else:
                topic= topicElement.text 

            for p_tag in soupArticle.find_all('p', {'class': 'ssrcss-1q0x1qg-Paragraph eq5iqo00'}):
                body = body + p_tag.text 
            text = body
            body = ""
            #text = soupArticle.find('div',{'class': 'ssrcss-7uxr49-RichTextContainer e5tfeyi1'}).text
            image = soupArticle.find('img')['src']
            authorsElement = soupArticle.find('div', {'class': 'ssrcss-68pt20-Text-TextContributorName'})
            if authorsElement == None : 
                authors = ""
            else:
                authors= authorsElement.text 
            df_new_row = pd.DataFrame({'title': title,'date':date,'subtitle':subtitle,'topic':topic,'text':text,'image':image,'authors':authors},index=[p])
        
            df = pd.concat([df, df_new_row])
            dataResponse.close()
            nextPage = soupArticle.find('a',{' class':'lx-pagination__btn qa-pagination-last-page lx-pagination__btn--active'})
            nextPageNumber = nextPage
            print(nextPage)
            p=p+1
            #new_var = int(newsPage.text)
            #new_var = int(newsPage.text) + 1

    with open('data.csv', 'w',encoding="utf-8") as f:
        df.to_csv(f, header=f.tell()==0)

scrape_data_task = PythonOperator(
    task_id='scrape_data_task',
    python_callable=scrape_data,
    dag=dag,
)
