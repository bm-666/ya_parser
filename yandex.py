from configparser import ConfigParser
from datetime import datetime as dt
from multiprocessing import Pool, Process
import time

import psycopg2
from  selenium import webdriver
from selenium.webdriver.chrome import options
from selenium.webdriver.common import desired_capabilities, keys


    
def date_convert(date_comment):
    '''Конвертация даты принимает строку с датой коментария '''
    x = date_comment.split(' ')
    month = {'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'}
    x[1] = month[date_comment.split(' ')[1]]  
    
    if len(x) > 2:
         date_comment = f'{x[0]}.{x[1]}.{x[2]}'    
    elif len(x) == 2:
        date_comment = f'{x[0]}.{x[1]}.{dt.now().year}'
    
    return date_comment 

def like(react, element):
    '''Функция получения количество Like/Dislike'''
    reaction = element.find_elements_by_class_name('business-reactions-view__container')[react].text
    if reaction != "":
        return reaction
    else:
        return 0        


def yandex_parse(url_tuple):
    '''Основная функция парсинга ЯндексКарт'''
    try:
        proxy = ['77.83.11.214:8000', '77.83.8.219:8000']
        userid = url_tuple[0]
        url = url_tuple[1]
        x = 0
        js_code = "return document.querySelector('.scroll__scrollbar-thumb').getBoundingClientRect();"
        date_parse = dt.strftime(dt.now(), '%d.%m.%Y')
        class_name = 'business-reviews-card-view__review'
        '''options = webdriver.ChromeOptions()
        options.add_argument('headless)'''
        content = webdriver.Chrome()
        content.get(url)
        raiting = content.find_element_by_class_name('business-rating-badge-view__rating-text').text   
        count_comments = int(content.find_element_by_class_name('business-header-rating-view__text').text.split(' ')[0])
        content.get(url+'/reviews/')
        content.find_element_by_class_name("flip-icon").click()
        parse_result = {
            userid:{
                "date": date_parse,
                "raiting": raiting,
                "count_comments": count_comments,
                "comments":[]
                }
            }
        
        if count_comments <= 50:
            result_comments = content.find_elements_by_class_name(class_name)
        elif 50 < count_comments <= 550:    
            result_comments = scroll(content, count_comments, class_name)
        else:
            count_comments = 550
            js_code_select_type = "document.querySelector('.flip-icon').click();\
                                return document.querySelectorAll('.rating-ranking-view__popup-line')[1].click();"
            content.execute_script(js_code_select_type)
            time.sleep(1)
            result_comments = scroll(content, count_comments, class_name)
        
        cmtid = 0
        for item in result_comments:
            cmtid += 1
            parse_result[userid]['comments'].append(
            ('yandex', cmtid, 'Опубликован',
            item.find_element_by_tag_name('span').text, 
            item.find_element_by_class_name("business-review-view__body-text").text,
            date_convert(item.find_element_by_class_name("business-review-view__date").text),
            like(0, item),
            like(1, item)
            ))             
    except Exception as e:
        print(e)
    finally:
        content.close()    
        
    return parse_result


def db_execute(result_parser, cursor):
    '''Вставка/Обновление в БД принимает результат парсинга dict, cursor'''
    for el in result_parser:
        cursor.execute(
            "INSERT INTO clientstat_yandexmapsmodel(date_parse, raiting, count_comments, yandex_key_id) VALUES (%s,%s, %s, %s)",
            (result_parser[el]['date'], result_parser[el]['raiting'],result_parser[el]['count_comments'], el)
            )        
        cursor.execute(f"SELECT count(*) FROM clientstat_commentsmodel WHERE  comments_key_id={el};")
        result_count = cursor.fetchone()
        if  result_count[0] == 0:
            for comment in result_parser[el]['comments']: 
                cursor.execute(
                    "INSERT INTO clientstat_commentsmodel (comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment, mylike, dislike, comments_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                    (comment + (el,)))
        else:
            coments_select = f"SELECT comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment FROM clientstat_commentsmodel WHERE comments_key_id={el} ;"  
            cursor.execute(coments_select)
            select_result = cursor.fetchall()           
            
            for cmt in result_parser[0][el]['comments']:
                if cmt[0:6] in select_result:
                    func_update_comment(cmt, cursor)
                else:
                    func_insert_comment(cmt, cursor, el)    


def func_update_comment(element, cursor):
    '''Обновление коменнатрия если он уже есть в БД'''
    sql_update = "UPDATE clientstat_commentsmodel SET mylike=%s, dislike=%s WHERE comment_number={}".format(element[1])
    cursor.execute(sql_update, (element[6],element[7]))


def func_insert_comment(element,cursor, userid):
    '''Функция вставка коментариев'''
    sql_insert = "INSERT INTO clientstat_commentsmodel (comment_resurs, comment_number, status_coment, author_comment, text_comment, date_comment, mylike, dislike, comments_key_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    element = element + (userid,)
    cursor.execute(sql_insert, element)


def scroll(content,count_comments, class_name):
        '''Функция скроллинга страницы.'''
        x = 0
        while count_comments > len(content.find_elements_by_class_name(class_name)):
            if len(content.find_elements_by_class_name(class_name)) >= 250:
                x = x - 200
            x = x + 350
            content.execute_script(f"document.querySelector('.scroll__container').scrollTo(0, {x});")
            time.sleep(1.5)
        result_comments = content.find_elements_by_class_name(class_name)
        
        return result_comments     


def run():
    try:
        config = ConfigParser()
        config.read("config.ini")
        dbname = config.get('DB', 'dbname')
        user = config.get('DB', 'user')
        password = config.get('DB', 'password')
        host = config.get('DB', 'host')
        port = config.get('DB', 'port')
        conn = psycopg2.connect(dbname=dbname,user=user, password=password, host=host, port=port)
        cur = conn.cursor()       
        cur.execute("SELECT id, url_yandex FROM clientstat_clientmodel;")
        yandex_list = cur.fetchall()
        
        main_pool = Pool(processes=1)
        result = main_pool.map(yandex_parse, yandex_list)
        for res in result:
            db_execute(res, cur)                
    
    except Exception as e:
        print(f"ERROR:{e}")
    finally:
        conn.commit()
        conn.close()


if __name__ == '__main__':
    run()


