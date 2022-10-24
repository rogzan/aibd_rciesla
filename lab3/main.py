import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if not isinstance(category_id, int):
        return None

    df = pd.read_sql("select film.title as title, language.name as languge, category.name as category\n"                      "from film inner join language on film.language_id = language.language_id\n"
                     "inner join film_category on film_category.film_id = film.film_id\n"
                     "inner join category on category.category_id = film_category.category_id\n"
                     "where category.category_id = {CATEGORY_ID} order by film.title, language.name\n"                        .format(**{"CATEGORY_ID": category_id}), con=connection)
    return df
    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(category_id, int):
        return None

    df = pd.read_sql("select category.name as category, count(film.film_id) as count from film\n"
                     "inner join film_category on film_category.film_id = film.film_id\n"
                     "inner join category on category.category_id = film_category.category_id\n"
                     "where category.category_id = {CATEGORY_ID} group by category.name\n"
                     .format(**{"CATEGORY_ID": category_id}), con=connection)

    return df

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if not (isinstance(min_length, int) or isinstance(min_length, float))\
            or not (isinstance(max_length, int) or isinstance(max_length, float))\
            or not min_length < max_length:
        return None
    
    df = pd.read_sql("select film.length as length, count(film.film_id) as count from film\n"
                     "where film.length between {MIN_LENGTH} and {MAX_LENGTH}\n"
                     "group by film.length\n"
                     .format(**{"MIN_LENGTH": min_length, "MAX_LENGTH": max_length}), con=connection)
    
    return df



def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if not isinstance(city, str):
        return None


    df = pd.read_sql("select city.city as city, customer.first_name as first_name,\n"
                     "customer.last_name as last_name from customer\n"
                     "inner join address on address.address_id = customer.address_id\n"
                     "inner join city on city.city_id = address.city_id where city.city = \'{CITY}\'\n"
                     "order by customer.last_name, customer.first_name\n"
                     .format(**{"CITY": city}), con=connection)

    return df

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy pożyczyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if not (isinstance(length, int) or isinstance(length, float)):
        return None

    df = pd.read_sql("select film.length as length, avg(payment.amount) as avg from film\n"
                     "inner join inventory on film.film_id = inventory.film_id\n"
                     "right outer join rental on inventory.inventory_id = rental.inventory_id\n"
                     "inner join payment on rental.rental_id = payment.rental_id\n"
                     "where film.length = {LENGTH} group by film.length\n"
                     .format(**{"LENGTH": length}), con=connection)

    return df



def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not (isinstance(sum_min, int) or isinstance(sum_min, float)):
        return None

    df = pd.read_sql("select customer.first_name as first_name, customer.last_name as last_name,\n"
                     "sum(film.length) as sum from customer\n"
                     "inner join rental on customer.customer_id = rental.customer_id\n"
                     "inner join inventory on inventory.inventory_id = rental.inventory_id\n"
                     "inner join film on inventory.film_id = film.film_id\n"
                     "group by customer.customer_id having sum(film.length) > {MIN_SUM}\n"
                     "order by sum(film.length), customer.last_name, customer.first_name\n"
                     .format(**{"MIN_SUM": sum_min}), con=connection)

    return df 

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if not isinstance(name, str):
        return None

    df = pd.read_sql("select category.name as category, avg(film.length) as avg,\n"
                     "sum(film.length) as sum, min(film.length) as min,\n"
                     "max(film.length) as max from category\n"
                     "inner join film_category on category.category_id = film_category.category_id\n"
                     "inner join film on film.film_id = film_category.film_id\n"
                     "where category.name = \'{CATEGORY}\' group by category.name\n"
                     .format(**{"CATEGORY": name}), con=connection)

    return df