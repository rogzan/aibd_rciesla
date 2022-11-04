import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if not isinstance(category, (int, str)):
        return None

    if isinstance(category, int):
        df = pd.read_sql("select film.title, language.name as languge, category.name as category from film\n"
                         "inner join language on film.language_id = language.language_id\n"
                         "inner join film_category ON film.film_id = film_category.film_id\n"
                         "inner join category on film_category.category_id = category.category_id\n"
                         "where category.category_id = {c}\n" 
                         "order by film.title, languge".format(c=category), con=connection)
        
    if isinstance(category, str):
        df = pd.read_sql("select film.title, language.name as languge, category.name as category from film\n"
                         "inner join language on film.language_id = language.language_id\n"
                         "inner join film_category on film.film_id = film_category.film_id\n"
                         "inner join category on film_category.category_id = category.category_id\n"
                         "where category.name like '\{c}\'\n" 
                         "order by film.title, languge".format(c=category), con=connection)
    
    return df
    
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category, (int, str)):
        return None

    if isinstance(category, int):
        df = pd.read_sql("select film.title, language.name as languge, category.name as category from film\n"
                         "inner join language on film.language_id = language.language_id\n"
                         "inner join film_category on film.film_id = film_category.film_id\n"
                         "inner join category on film_category.category_id = category.category_id\n"
                         "where category.category_id = {c}\n" 
                         "order by film.title, languge".format(c=category), con=connection)
        return df
        
    if isinstance(category, str):
        df = pd.read_sql("select film.title, language.name as languge, category.name as category from film\n"
                         "inner join language on film.language_id = language.language_id\n"
                         "inner join film_category on film.film_id = film_category.film_id\n"
                         "inner join category on film_category.category_id = category.category_id\n"
                         "where category.name ilike'\{c}\'\n" 
                         "order by film.title, languge".format(c=category), con=connection)
    
        return df
    

    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if not isinstance(title, str):
        return None
    
    else:
        df = pd.read_sql("select actor.first_name, actor.last_name from film\n"   
                         "inner join film_actor on film.film_id = film_actor.film_id\n"    
                         "inner join actor on film_actor.actor_id = actor.actor_id\n"  
                         "where film.title like '\{t}\'\n"  
                         "order by actor.last_name, actor.first_name".format(t=title), con=connection)
        return df
    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(words, list):
        return None 

    else:
        q = f"'( |^)("
        tmp = "|".join(words)
        q += tmp
        q += ")+( |$)'"


        df = pd.read_sql("select film.title from film where film.title ~* {r_q}\n"
                         "order by film.title".format(r_q=q), con=connection)

        return df