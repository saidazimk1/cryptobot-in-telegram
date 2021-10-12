#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import random
import re
import sqlite3
import json
import sys
import time

import requests
import telebot
import urllib3

from settings import *

TABLE_NAME = 'stoks'

DEBUG = False  # показывать или нет расширенную информацию (True/False)

conn = sqlite3.connect(r'stocks.db')

bot = telebot.TeleBot(BOT_TOKEN)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/94.0.4606.81 Safari/537.36'

PAUSE_CYCLE_DEFAULT = 30  # пауза по умолчанию между проверкой новых цен, задается в секундах
TIME_ONLY_PRICE_DEFAULT = 60*60*4  # каждые N секунд (60*60*4 = 4 часа)  отправлять уведомление о текущих ценах
COUNTER_SEND_DEFAULT = 2000  # каждые 2000 циклов отправлять в ТГ уведомление, что бот работает в штатном режиме.


def pause(delay, rand=False):
    if delay == 0:
        return
    if rand:
        if random.randint(1, 3) == 1:
            sign = -1
        else:
            sign = 1
        rand_delay = random.randint(1, delay) / 5
        delay = delay + sign * rand_delay
    time.sleep(delay)


def log(s, need_to_write: bool = True):
    now = datetime.datetime.now()
    msg = f'{now.strftime("%Y-%m-%d %H:%M:%S")}: {s}'
    print(msg)
    if need_to_write:
        fn = f'{now.strftime("%Y%m%d")}_log.txt'
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        fn = os.path.join(log_dir, fn)
        with open(fn, 'a', encoding='utf-8') as f:
            f.write(f'{msg}\n')


def clear_old_logs():
    fn = f'{(datetime.datetime.now() - datetime.timedelta(days=3)).strftime("%Y%m%d")}_log.txt'
    if os.path.exists(fn):
        os.remove(fn)


def pars(source_string, start, end):
    find_string = ''
    if (start in source_string) and (end in source_string):
        try:
            find_string = source_string.split(start)[1]
            find_string = find_string.split(end)[0]
        except:
            pass
    find_string = re.sub(r"^\s+|\n|\r|\s+$", ' ', find_string)

    return find_string


def send_telegram_msg(text: str, chat_id: str):
    for try_num in range(10):
        try:
            bot.send_message(chat_id, text, parse_mode='Markdown')
            return True
        except Exception as e:
            log(f'Ошибка отправки уведомления в ТГ: {e}. Попробуем снова через 5 секунд...')
            pause(5, False)

    log('Не удалось отправить сообщение даже после 10 попыток!')
    return False


def send_notification(changed_tickets):
    log(f'Есть уведомления для отправки: \n{changed_tickets}')
    msg = '\n'.join(changed_tickets)
    send_telegram_msg(msg, CHAT_ID)


def convert_value_to_float(val):
    if not val:
        return None

    try:
        value_float = float(str(val).strip())
    except Exception as e:
        log(f'Ошибка перевода значения "{val}" в число: {e}')
        value_float = None

    return value_float


def create_table():
    cursor = conn.cursor()
    cursor.execute(f"""
               CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                   tiket_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                   ticket_name      TEXT,
                   delta_accept     DOUBLE,
                   price            DOUBLE,
                   date_update      DATETIME
               )
           """)
    conn.commit()


def read_tickets_from_base():
    """ Получить из базы ранее сохраненные тикеты """

    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    tickets_in_base = {}
    for ticket in cursor.fetchall():
        if ticket:
            tickets_in_base[ticket[1]] = {'delta_accept': ticket[2], 'price': ticket[3]}

    return tickets_in_base


def update_ticket_in_base(ticket_name: str, delta_accept: float, price=None):
    """ Обновить информацию о тикете в базе """
    cursor = conn.cursor()

    if price:
        sql_update_query = f""" UPDATE {TABLE_NAME} 
                                SET delta_accept = ?, price = ?, date_update = ? 
                                WHERE ticket_name = ? """
        data = (delta_accept, price, datetime.datetime.now(), ticket_name)
    else:
        sql_update_query = f""" UPDATE {TABLE_NAME} 
                                        SET delta_accept = ?, date_update = ? 
                                        WHERE ticket_name = ? """
        data = (delta_accept, datetime.datetime.now(), ticket_name)
    try:
        cursor.execute(sql_update_query, data)
        res = True
    except Exception as e:
        log(f'Ошибка обновления тикета в базе: {e}')
        res = False

    conn.commit()
    return res


def add_ticket_to_base(ticket_name: str, delta: float, price=None):
    cursor = conn.cursor()
    try:
        cursor.execute(f"INSERT INTO {TABLE_NAME} (ticket_name, delta_accept, price, date_update)"
                       f"VALUES (?, ?, ?, ?)",
                       (
                           ticket_name,
                           delta,
                           price,
                           datetime.datetime.now()
                       ))
        conn.commit()
        return True
    except Exception as e:
        log(f'Ошибка добавления тикета "{ticket_name}" в базу: {e}')
        return False


def add_tickets_to_base(tickets):
    """ Добавить все тикеты в базу """
    for ticket_name, ticket_rate in tickets.items():
        add_ticket_to_base(ticket_name, ticket_rate)


def actualize_table(tickets: dict):
    if DEBUG:
        log('Считываем данные из файла и проверяем на появление в списке новых тикетов')

    tickets_from_base = read_tickets_from_base()

    for ticket_name, ticket_info in tickets.items():
        price_now = ticket_info.get('price')
        delta_now = convert_value_to_float(ticket_info.get('delta_accept'))

        if not tickets_from_base.get(ticket_name):
            add_ticket_to_base(ticket_name=ticket_name, delta=delta_now, price=price_now)
        else:
            delta_in_base = tickets_from_base[ticket_name].get('delta_accept')
            price_in_base = tickets_from_base[ticket_name].get('price')

            is_delta_change = delta_now != delta_in_base
            is_price_change = price_now != price_in_base
            if is_delta_change or is_price_change:
                if update_ticket_in_base(ticket_name=ticket_name, delta_accept=delta_now, price=price_now):
                    if is_delta_change and delta_in_base and delta_now:
                        log(f'Произошло обновление дельты в базе для "{ticket_name}": {delta_in_base} -> {delta_now}')

                    if is_price_change and price_in_base and price_now and DEBUG:
                        log(f'Произошло обновление цены в базе для "{ticket_name}": {price_in_base} -> {price_now}')


def read_all_tickets(fn):
    if not os.path.isfile(fn):
        log(f'Не найден файл со списком тикетов: {fn}')
        return None

    try:
        with open(fn) as f:
            all_tickets = json.load(f)
    except Exception as e:
        log(f'Ошибка чтения списка тикетов: {e}')
        return None

    return all_tickets


def read_config(fn):
    if not os.path.isfile(fn):
        log(f'Не найден файл со списком кофигурационных данных: {fn}')
        all_configs = None
    else:
        try:
            with open(fn) as f:
                all_configs = json.load(f)
        except Exception as e:
            log(f'Ошибка чтения списка настроек: {e}')
            all_configs = None

    if not all_configs:
        log('Используем настройки по умолчанию...')
        pause_cycle = PAUSE_CYCLE_DEFAULT
        time_only_price = TIME_ONLY_PRICE_DEFAULT
        counter_send = COUNTER_SEND_DEFAULT
    else:
        pause_cycle = all_configs.get('PAUSE_CYCLE', PAUSE_CYCLE_DEFAULT)
        time_only_price = all_configs.get('TIME_ONLY_PRICE', TIME_ONLY_PRICE_DEFAULT)
        counter_send = all_configs.get('COUNTER_SEND', COUNTER_SEND_DEFAULT)

    return pause_cycle, time_only_price, counter_send


def get_all_prices_binance():
    """ Получить текущие рыночные цены на BINANCE """
    log(f'{"="*10}>Идет получение цен через сервис "Binance"...')

    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/json, text/javascript, */*; q=0.01'}

    url = f'https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list'
    try:
        response = requests.get(url, headers=headers).json().get('data')
    except Exception as e:
        log(f'Не удалось получить информацию по ценам через сервис "Moex": {e}')
        response = None

    return response


def get_price_for_ticket(all_current_items, ticket):
    ticket = ticket.lower().strip()
    for item in all_current_items:
        name = item.get('name', '').lower()
        if name == ticket:
            price = item.get('price')
            return price

    log(f'Не удалось получить информацию для тикета: {ticket}')
    return None


def get_current_prices(all_tickets):
    """ Получить список цен для всех тикетов """
    if DEBUG:
        log('Получение текущего списка цен...')
    all_current_items = get_all_prices_binance()
    if not all_current_items:
        return None

    tickets_now = {}
    for ticket_name, ticket_info in all_tickets.items():
        # ticket_price = convert_value_to_float(get_ticket_price(ticket_name))
        ticket_price = get_price_for_ticket(all_current_items, ticket_name)
        if ticket_price:
            delta_now = convert_value_to_float(ticket_info.get('delta_accept'))
            tickets_now[ticket_name] = {'delta_accept': delta_now, 'price': ticket_price}

    return tickets_now


def process_one_cycle(all_tickets):
    currents_tickets = get_current_prices(all_tickets)
    if not currents_tickets:
        send_telegram_msg('Ошибка получения списка текущих цен. '
                          'Требуется настройка скрипта! Работа будет остановлена!', CHAT_ID)
        sys.exit()

    tickets_from_base = read_tickets_from_base()

    changed_tickets = []
    all_prices = []  # список текущих цен для всего набора тикетов
    for ticket_name, ticket_info in currents_tickets.items():
        try:
            price_now = ticket_info.get('price')
            delta_accept = ticket_info.get('delta_accept')
            price_in_base = tickets_from_base[ticket_name].get('price')
        except Exception as e:
            log(f'Ошибка сравнения для "{ticket_name}": {e}')
            continue

        if price_in_base:
            current_delta = price_now - price_in_base
            change = None
            if current_delta > delta_accept:
                if current_delta > 0:
                    change = 'увеличился'
                elif current_delta < 0:
                    change = 'увеличился'

            if change:
                changed = f'*{ticket_name}* - {change} на {round(abs(current_delta), 2)}. ' \
                          f'Предыдущая: {price_in_base}. Текущая: {price_now}. Дельта: {round(current_delta, 2)}'
                changed_tickets.append(changed)
            else:
                log(f'{ticket_name} - Текущая: {price_now}. Предыдущая: {price_in_base}. '
                    f'Дельта: {round(current_delta, 2)}. Допустимая дельта: {delta_accept}')

            all_prices.append(f'{ticket_name}: текущая цена = {price_now}.')

    actualize_table(currents_tickets)

    return changed_tickets, all_prices


def main():
    log('Начало работы...')
    send_telegram_msg('Бот для ослеживания курсов валют запущен!', CHAT_ID)

    create_table()

    cycle_num = 1
    start_time = time.time()
    while True:
        pause_cycle, time_only_price, counter_send = read_config(CONFIG_FN)

        all_tickets = read_all_tickets(TICKETS_FN)
        actualize_table(all_tickets)

        log(f'\n{"=" * 50}> Выполняется цикл проверки №{cycle_num}')
        if cycle_num % counter_send == 0 and cycle_num > 0:
            clear_old_logs()
            send_telegram_msg('Бот работает в штатном режиме...', CHAT_ID)

        changed_tickets, all_prices = process_one_cycle(all_tickets)

        if changed_tickets:
            send_notification(changed_tickets)
        else:
            if round(time.time() - start_time) < time_only_price:
                log('Нет уведомлений для отправки.')
            else:
                send_notification(all_prices)
                start_time = time.time()

        log('Пауза перед следующим циклом получения цен...')
        pause(pause_cycle)
        cycle_num += 1


if __name__ == '__main__':
    main()
