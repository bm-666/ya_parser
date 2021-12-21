from configparser import ConfigParser
from datetime import datetime as dt
from multiprocessing import Pool
import time


import psycopg2
from selenium.webdriver import chrome
from selenium.webdriver.chrome import options
from selenium.webdriver.common import desired_capabilities, keys
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
  
    

def date_convert(args):
    try:
        '''Конвертация даты принимает строку с датой коментария '''
        x = args.split()
        month = {'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'}
        x[1] = month[x[1]]  
        if len(x) > 2:
            new_date = '.'.join(x) 
        elif len(x) == 2:
            x.append(str(dt.now().year))
            new_date = '.'.join(x)
    except IndexError:
        return 

    return new_date 


def like(react, element):
    '''Функция получения количество Like/Dislike'''
    reaction = element.find_elements(By.CLASS_NAME,'business-reactions-view__container')[react].text
    if reaction != "":
        return reaction
    else:
        return 0   

def scroll(class_name, content, count_cooments):
        '''Функция скроллинга страницы.'''
        
        
        if count_cooments < 50:
            cmt = count_cooments
        else:
            cmt = 50          
        coord = 0
        result = set()       
        while len(result) < cmt:
            coord = coord + 100
            content.execute_script(f"document.querySelector('.scroll__container').scrollTo(0, {coord});")
            time.sleep(5)
            elements = {i for i in content.find_elements(By.CLASS_NAME, class_name)}
            result = result.union(elements)
        result_comments = [i for i in result]
        return result_comments 


def yandex_parse(url_tuple):
    '''Основная функция парсинга ЯндексКарт'''
    userid = url_tuple[0]
    url = url_tuple[1]
    js_code = "return document.querySelector('.scroll__scrollbar-thumb')getBoundingClientRect();"      
    js_code_select_new = "return document.querySelectorAll('.rating-ranking-view__popup-line')[1].click();"
    date_parse = dt.strftime(dt.now(), '%d.%m.%Y')
    class_name = 'business-reviews-card-view__review'
    try:
        options_chrome = webdriver.ChromeOptions()
        options_chrome.add_argument('--headless')
        options_chrome.add_argument('--no-sandbox')
        content = webdriver.Chrome(options=options_chrome)
        content.get(url)
        raiting = content.find_element(By.CLASS_NAME, 'business-rating-badge-view__rating-text').text
        content.execute_script("return document.querySelector('._name_reviews').click();")
        time.sleep(5)
        content.execute_script("document.querySelector('.scroll__container').scrollTo(0, 1000);")
        count_comments = int(content.find_elements(By.CLASS_NAME,'card-section-header__title')[-1].text.split()[0])
        parse_result = {
            userid:{
                "date": date_parse,
                "raiting": raiting,
                "count_comments": count_comments,
                "comments":[]
                }
            }               
        content.execute_script('return document.querySelectorAll(".business-reviews-card-view__ranking")[1].querySelector(".flip-icon").click();')
        content.execute_script(js_code_select_new)
        time.sleep(7)
        '''content.execute_script("document.querySelector('.scroll__container').scrollTo(0, 200);")'''
        result_comments = scroll(class_name, content, count_comments)
        cmtid = url_tuple[2]
        for item in result_comments:
            stars = 5
            if item.text == "":
                continue
            else:
                cmtid += 1
                date_comment = item.find_element(By.CLASS_NAME,"business-review-view__date").text
                author = item.find_element(By.TAG_NAME,'span').text
                text = item.find_element(By.CLASS_NAME,'business-review-view__body-text').text
                stars_empty=item.find_element(By.CLASS_NAME, 'business-rating-badge-view__stars').find_elements(
                    By.CLASS_NAME, '_empty')
                stars = stars - len(stars_empty)
                parse_result[userid]['comments'].append(
                ('yandex', cmtid, 'Опубликован',
                author,
                text,
                date_convert(date_comment),
                like(0, item),
                like(1, item),
                stars
                ))    
    except Exception as e:
        print(e)
    finally:
        content.close()    
    return parse_result

    
def db_execute(result_parser, cursor):
    '''Вставка БД принимает результат парсинга dict, cursor'''
    for el in result_parser:
        cursor.execute(
            "INSERT INTO yandex(date_parse, raiting, count_comments, table_key_id) VALUES (%s,%s, %s, %s);",
            (result_parser[el]['date'], result_parser[el]['raiting'],result_parser[el]['count_comments'], el)
            )        
        for comment in result_parser[el]['comments']: 
            cursor.execute(
                "INSERT INTO comments (comment_resurs, comment_number, status_comment, author_comment, text_comment, date_comment,mylike, dislike,comment_stars, comment_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
               (comment + (el,)))    

def func_update_comment(element, cursor):
    '''Обновление коменнатрия если он уже есть в БД'''
    sql_update = "UPDATE comments SET mylike=%s, dislike=%s WHERE comment_number={}".format(element[1])
    cursor.execute(sql_update, (element[6],element[7]))


def func_insert_comment(element,cursor, userid):
    '''Функция вставка коментариев'''
    sql_insert = "INSERT INTO comments (comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment, mylike, dislike, comment_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    element = element + (userid,)
    cursor.execute(sql_insert, element)


def count_comment(args):
    if args[2] is None:
        args = args[0:2]+(0,)
    return args

def start_parser():
    try:
        config = ConfigParser()
        config.read('config.ini')
        dbname = config.get('DB', 'dbname')
        user = config.get('DB', 'user')
        password = config.get('DB', 'password')
        host = config.get('DB', 'host')
        port = config.get('DB', 'port')
        conn = psycopg2.connect(dbname=dbname,user=user, password=password,host=host, port=port)
        cur = conn.cursor()       
        cur.execute(
            "SELECT c.id, c.url_yandex, (SELECT comment_number FROM comments WHERE comment_key_id = c.id ORDER BY comment_number DESC LIMIT 1) FROM clients c;"
            )
        yandex_list = [count_comment(i) for i in cur.fetchall()]
        main_pool = Pool(2)
        result = main_pool.map(yandex_parse, yandex_list)
        for res in result: 
            db_execute(res, cur)
                            
    except Exception as error:
        print(error)
    finally:
        conn.commit()
        conn.close()

if __name__ == '__main__':
    start_parser()


