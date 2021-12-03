from configparser import ConfigParser
from datetime import datetime as dt
from multiprocessing import Pool
import time
import random

import psycopg2
from selenium.webdriver import chrome
from selenium.webdriver.chrome import options
from selenium.webdriver.common import desired_capabilities, keys
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
    reaction = element.find_elements_by_class_name('business-reactions-view__container')[react].text
    if reaction != "":
        return reaction
    else:
        return 0        
        return result_comments     
        return result_comments     
        return result_comments     

def scroll(class_name, content):
        '''Функция скроллинга страницы.'''
        coord = 0
        while len(content.find_elements_by_class_name(class_name)) <= 50:
            coord = coord + 350
            content.execute_script(f"document.querySelector('.scroll__container').scrollTo(0, {coord});")
            time.sleep(2)
        result_comments = content.find_elements_by_class_name(class_name)
        return result_comments 


def yandex_parse(url_tuple):
    '''Основная функция парсинга ЯндексКарт'''
    userid = url_tuple[0]
    url = url_tuple[1]
    count_result = url_tuple[2]
    '''options = {
            "proxy" : {
                "https" : url_tuple[3]
            }
        }'''
    js_code = "return document.querySelector('.scroll__scrollbar-thumb')getBoundingClientRect();"
    js_code_select_type = 0       
    js_code_select_new = "return document.querySelectorAll('.rating-ranking-view__popup-line')[1].click();"
    date_parse = dt.strftime(dt.now(), '%d.%m.%Y')
    class_name = 'business-reviews-card-view__review'
    try:
        options_chrome = webdriver.ChromeOptions()
        options_chrome.add_argument('headless')
        options_chrome.add_argument("window-size=2560,1440")
        content = webdriver.Chrome(options=options_chrome)
        content.get(url)
        raiting = content.find_element_by_class_name('business-rating-badge-view__rating-text').text
        content.execute_script("return document.querySelector('._name_reviews').click();")
        time.sleep(5)
        content.execute_script("document.querySelector('.scroll__container').scrollTo(0, 1000);")
        count_comments = int(content.find_elements_by_class_name('card-section-header__title')[-1].text.split()[0])
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
        content.execute_script("document.querySelector('.scroll__container').scrollTo(0, 1000);")
        time.sleep(3)
        result_comments = scroll(class_name, content)
        cmtid = 0
        for item in result_comments:
            cmtid += 1
            if item.text == "":
                continue
            else:
                date_comment = item.find_element_by_class_name("business-review-view__date").text
                author = item.find_element_by_tag_name('span').text
                text = item.find_element_by_class_name("business-review-view__body-text").text
                parse_result[userid]['comments'].append(
                ('yandex', cmtid, 'Опубликован',
                author,
                text,
                date_convert(date_comment),
                like(0, item),
                like(1, item)
                ))    
    except Exception as e:
        print(e)
    finally:
        content.close()
    result = (parse_result, count_result)    
    return result


def type_parser(cursor, el, config):
    "Функция принимает cursor, id клиента, config возвращает кортеж"
    cursor.execute(f"SELECT count(*) FROM comments WHERE  comments_key_id={el[0]};")
    proxy_config = ""
    result_count = cursor.fetchone()
    if result_count[0] > 0:
        return el + (True, proxy_config)
    else:
        return el + (False, proxy_config)    

    
def db_execute(result_parser,  count_result, cursor):
    '''Вставка/Обновление в БД принимает результат парсинга dict, cursor'''
    for el in result_parser:
        cursor.execute(
            "INSERT INTO yandex(date_parse, raiting, count_comments, yandex_key_id) VALUES (%s,%s, %s, %s);",
            (result_parser[el]['date'], result_parser[el]['raiting'],result_parser[el]['count_comments'], el)
            )        
        if count_result is False:
            for comment in result_parser[el]['comments']: 
                cursor.execute(
                    "INSERT INTO comments (comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment, mylike, dislike, comments_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                    (comment + (el,)))
        else:
            coments_select = f"SELECT comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment FROM comments WHERE comments_key_id={el} ;"  
            cursor.execute(coments_select)
            select_result = cursor.fetchall()           
            
            for cmt in result_parser[0][el]['comments']:
                if cmt[0:6] in select_result:
                    func_update_comment(cmt, cursor)
                else:
                    func_insert_comment(cmt, cursor, el)    


def func_update_comment(element, cursor):
    '''Обновление коменнатрия если он уже есть в БД'''
    sql_update = "UPDATE comments SET mylike=%s, dislike=%s WHERE comment_number={}".format(element[1])
    cursor.execute(sql_update, (element[6],element[7]))


def func_insert_comment(element,cursor, userid):
    '''Функция вставка коментариев'''
    sql_insert = "INSERT INTO comments (comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment, mylike, dislike, comments_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    element = element + (userid,)
    cursor.execute(sql_insert, element)


def start_parser():
    try:
        config = ConfigParser()
        config.read("/home/bm666/Рабочий стол/test_parse/config.ini")
        dbname = config.get('DB', 'dbname')
        user = config.get('DB', 'user')
        password = config.get('DB', 'password')
        host = config.get('DB', 'host')
        port = config.get('DB', 'port')
        conn = psycopg2.connect(dbname=dbname,user=user, password=password,host=host, port=port)
        cur = conn.cursor()       
        cur.execute("SELECT id, url_yandex FROM clients;")
        yandex_list = [type_parser(cur, i, config) for i in cur.fetchall()]     
        main_pool = Pool(processes=2)
        result = main_pool.map(yandex_parse, yandex_list)
        for res in result: 
            db_execute(res[0],res[1], cur)                
    
    except Exception as error:
        print(error)
    finally:
        conn.commit()
        conn.close()

if __name__ == '__main__':
    start_parser()


