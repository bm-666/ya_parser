import requests
import time
import json
import os
import math
from socket import timeout
from configparser import ConfigParser
from datetime import date, datetime as dt
from multiprocessing import Pool
from requests import Session
from urllib.parse import urlparse, urlencode


import psycopg2
from selenium.webdriver import chrome
from selenium.webdriver.chrome import options
from selenium.webdriver.common import desired_capabilities, keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver






comments = {}

def connect_db(func):
    '''
    Декоратор возвращает соединение с бд.
    '''
    def wrap(*args):
        config = ConfigParser()
        config.read('config.ini')
        dbname = config.get('DB', 'dbname')
        user = config.get('DB', 'user')
        password = config.get('DB', 'password')
        host = config.get('DB', 'host')
        port = config.get('DB', 'port')
        conn = psycopg2.connect(dbname=dbname, user=user,
                                password=password, host=host, port=port)

        conn.autocommit = True
        func(conn,*args)
    return wrap

def count_pages(count_comments:int):
    page = 1
    comments_page = 50
    while count_comments > comments_page:
        page += 1
        count_comments -= comments_page
    
    return page

def load_element(driver, locator):
    # Ожидание загрузки элемента    
    timeout = 40
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(locator))
    except:
        element = None    
    return element

def load_elements(driver, locator):
    timeout = 40
    try:
        elements = WebDriverWait(driver, timeout).until(EC.presence_of_all_elements_located(locator))
    except Exception:
        elements = None
    
    return elements        

def date_convert(args:str):
    # Приведение даты к формату DD.MM.YYYY
    try:
        list_args = args.split()
        month = {'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06',
                 'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'}
        list_args[1] = month[list_args[1]]
        if len(list_args) > 2:
            new_date = '.'.join(list_args)
        elif len(list_args) == 2:
            list_args.append(str(dt.now().year))
            new_date = '.'.join(list_args)
    except IndexError:
        return

    return new_date


def like(react, element):
    '''Функция получения количество Like/Dislike'''
    reaction = element.find_elements(
        By.CLASS_NAME, 'business-reactions-view__container')[react].text
    if reaction != "":
        return reaction
    else:
        return 0



def scroll(class_name, content, count_comments):
    #Прокручивание динамически загружаемых коментариев    
    comments = []
    
    result = set()
    load = load_element(content,(By.CLASS_NAME, class_name))
    
    if load is  not None:
        if count_comments < 50:
            cmt = count_comments
        else:
            cmt = 50
        coord = 0
        while len(result) < cmt:
            coord = coord + 100
            content.execute_script(
                f"document.querySelector('.scroll__container').scrollTo(0, {coord});")
            time.sleep(3)
            elements = {i for i in content.find_elements(
                By.CLASS_NAME, class_name)}
            result = result.union(elements)
        
        result_comments = [i for i in result]
        
    
    return result_comments

def driver(width=1366,hight=768):
    # Возвращает instance webdriver обязательные параметры ширина высота браузера
    
    options_chrome = webdriver.ChromeOptions()
    options_chrome.add_argument('--headless')
    options_chrome.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options_chrome)
    driver.set_window_size(width,hight)
    return driver

def yandex_parse(args:tuple):
    #Функция парсинга страницы ЯК, принимет кортеж элементов id, url страницы, номер последнего коментария.
    
    #CSS-локаторы для поиска элементов на странице и код JS для запуска в браузере
    
    class_raiting_block = (By.CLASS_NAME, 'business-reviews-card-view__title')
    class_css_flip = (By.CLASS_NAME, 'business-reviews-card-view__ranking')
    class_name = 'business-reviews-card-view__review'
    css_class_raiting = (
            By.CLASS_NAME, 'business-rating-badge-view__rating-text')
    css_class_header = (By.CLASS_NAME, 'business-reviews-card-view__ranking')
    js_code_select_views_new = "return document.querySelectorAll('.rating-ranking-view__popup-line')[1].click();"
    js_code_click_sort_views = 'return document.querySelectorAll(".business-reviews-card-view__ranking")[1].querySelector(".flip-icon").click();'
    
    userid, url, cmtid = args
    date_parse = dt.strftime(dt.now(), '%d.%m.%Y')
    
    try:
        content = driver()
        content.get(url)
        load_element(content, class_raiting_block)
        raiting = load_element(content, css_class_raiting)
        
        if raiting is not None:
            raiting = raiting.text
        else:
            raiting = '0,0'                

        content.execute_script(
            "return document.querySelector('._name_reviews').click();")
        count_comments = int(load_element(content, (By.CLASS_NAME, 'business-reviews-card-view__title')).text.split()[0])
        content.execute_script(
            "document.querySelector('.scroll__container').scrollTo(0, 1000);")             
        
        parse_result = {
            userid: {
                "date": date_parse,
                "raiting": raiting,
                "count_comments": count_comments,
                "comments": []
            }
        }
        
        default_reviews = load_element(content, class_css_flip)
        time.sleep(10)
        try:
            if default_reviews is not None:
                default_reviews.find_element(By.CLASS_NAME, 'flip-icon').click()
        except:
            content.execute_script(js_code_click_sort_views)
            content.execute_script(js_code_select_views_new)
        header = load_element(content, css_class_header)
        if header is not None:
            result_comments = scroll(class_name, content, count_comments)
        
        for item in result_comments:
            stars = 5
            if item.text == "":
                continue
            else:
                cmtid += 1
                date_comment = item.find_element(
                    By.CLASS_NAME, "business-review-view__date").text
                author = item.find_element(By.TAG_NAME, 'span').text
                text = item.find_element(
                    By.CLASS_NAME, 'business-review-view__body-text').text
                stars_empty = item.find_element(By.CLASS_NAME, 'business-rating-badge-view__stars').find_elements(
                    By.CLASS_NAME, '_empty')
                stars = stars - len(stars_empty)
                parse_result[userid]['comments'].append(
                    (
                    "yandex", cmtid, 'Опубликован', author, text,
                    date_convert(date_comment),like(0, item), like(1, item),
                    stars
                ))
    
        if len(parse_result) == 0:
            return 0
        removed_reviews(parse_result[userid]["comments"],userid)    
    except Exception as e:
        print(e, url)
    
    finally:
        content.close()
    
    return parse_result


def db_execute(result_parser, cursor):
    '''Вставка БД принимает результат парсинга dict, cursor'''
    if result_parser == 0:
        return 
    
    for el in result_parser:
        cursor.execute(
            "INSERT INTO yandex(date_parse, raiting, count_comments, table_key_id) VALUES (%s,%s, %s, %s);",
            (result_parser[el]['date'], result_parser[el]
             ['raiting'], result_parser[el]['count_comments'], el)
        )
        for comment in result_parser[el]['comments']:
            cursor.execute(
                "INSERT INTO comments (comment_resurs, comment_number, status_comment, author_comment, text_comment, date_comment,mylike, dislike,comment_stars, comment_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                (comment + (el,)))

                            


@connect_db
def removed_reviews(*args):
    #Находим удаленные отзывы и обновляем забись в бд
    SQL_SELECT_COMMENTS = "SELECT comment_resurs, author_comment, text_comment FROM comments WHERE status_comment='Опубликован' AND comment_key_id=%s;"
    
    connect, new_result, userid = args
    cur = connect.cursor()
    new_result = set((i[0],i[3],i[4]) for i in new_result)
    cur.execute(SQL_SELECT_COMMENTS, (userid,))
    old_comments = set(cur.fetchall())
    reviews = old_comments.difference(new_result)
    
    if len(reviews) > 0:
        update_remowed_reviews(cur, reviews)

    

def update_remowed_reviews(cursor, args):
    #Обновляем коментарии устанавливаем дату удаления и статус принимает cursor,  и множество кортежей коментариев на удаление.
    date_delete =  dt.strftime(dt.now(), '%d.%m.%Y')
    SQL_UPDATE_DELETE_REVIEWS = "UPDATE comments SET status_comment='Удален', date_delete=%s WHERE comment_resurs=%s AND author_comment=%s AND text_comment=%s"
    for i in args:
        cursor.execute(SQL_UPDATE_DELETE_REVIEWS, ((date_delete,)+i))

        


def count_comment(args):
    if args[2] is None:
        args = args[0:2]+(0,)
    return args

    

@connect_db
def start_parser(*args):
    try:
        conn = args[0]
        cur = conn.cursor()
        cur.execute(
            "SELECT c.id, c.url_yandex, (SELECT comment_number FROM comments WHERE comment_key_id = c.id ORDER BY comment_number DESC LIMIT 1) FROM clients c WHERE c.status='active';"
        )
        yandex_list = [count_comment(i) for i in cur.fetchall()]
        with Pool(1) as pool:
            result = pool.map(yandex_parse, yandex_list)
        for res in result: 
            db_execute(res, cur)    
    except Exception as error:
        print(error)
    finally:
        conn.close()


if __name__ == '__main__':
    start_parser()
