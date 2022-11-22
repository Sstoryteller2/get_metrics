from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import numpy as np
import config
import json
import io


# Результаты получаем в тысячах
def cut_num1(num):
    return f"{num/1000:.1f}"


def cut_num2(num):
    return f"{num/1000:.2f}"


def reduce_nums(mylist, nl):
    if nl == 1:
        mylist = [cut_num1(float(x)) for x in mylist]
    if nl == 2:
        mylist = [cut_num2(float(x)) for x in mylist]
    return mylist


# попарное вычитание
def get_derev(lst):
    derev = list()
    for c, i in enumerate(lst):
        if c != len(lst) - 1:
            derev.append((lst[c + 1]) - i)
    return derev


# Трафик по неделям
def get_trafic_data(counter_n, date1, date2):
    api_url = "https://api-metrika.yandex.net/stat/v1/data/bytime"
    payload = {
        "id": counter_n,
        "date1": date1,
        "date2": date2,
        "metrics": "ym:s:visits",
        "accuracy": "full",
        "dimensions": "ym:s:<attribution>TrafficSource",
        "lang": "ru",
        "group": config.group,
     #   "filters": "EXISTS(ym:s:regionAreaName == 'Название региона')",
        # 'limit':150
    }
    resp = requests.get(api_url, headers=config.header_params, params=payload)
    return json.loads(resp.text)


# заголовки за период
def get_headlines_data(counter_n, date1, date2, limit):
    api_url = "https://api-metrika.yandex.net/stat/v1/data"
    payload = {
        "id": counter_n,
        "date1": date1,
        "date2": date2,
        "metrics": "ym:pv:users",
        "accuracy": "full",
        "dimensions": "ym:pv:title",
        "lang": "ru",
     #   "filters": "EXISTS(ym:s:regionAreaName == 'Название региона')",
        "limit": limit,
    }
    response_headlines = requests.get(
        api_url, headers=config.header_params, params=payload
    )
    return json.loads(response_headlines.text)


def hl_arr(headlines):
    A = np.zeros((0, 2))
    hl_lst = list()
    for item in headlines["data"]:        
        hl_lst = [item["dimensions"][0]["name"], f"{item['metrics'][0]/1000:,.1f}"]
        A = np.vstack([A, hl_lst])
    return A


def get_arrays(trafic):
    Trafic22Array = np.zeros((0, trafic["total_rows"] + 2))  # массив для данных
    row = list()
    # заполняем данными
    for c, i in enumerate(trafic["data"]):
        name = i["dimensions"][0]["name"]
        row = [name]  # название источника трафика
        data_list = i["metrics"][0].copy()  # собераем данные в отдельные список
        data_list.append(sum(data_list))  # суммируем данные (всего за период)
        row.extend(data_list)
        Trafic22Array = np.vstack([Trafic22Array, row])
    # добавляем строчку с суммами
    sum_row = ["всего"]
    for c, i in enumerate(Trafic22Array[0]):
        if c != 0:
            sum_row.append(Trafic22Array[:, c].copy().astype(float).sum())
            d_sum_row = ["всего"] + get_derev(sum_row[1:-1])
            d_sum_row.append(sum(d_sum_row[1:]))

    Trafic22Array = np.vstack([Trafic22Array, sum_row])
    return Trafic22Array


def get_sum(array):
    height = array.shape[0] - 1
    width = array.shape[1] - 1
    return array[height][
        width
    ]  # правый нижний угол таблицы суммарный трафик со всех источников


def get_details_array(Trafic22Array):
    Period_Details_Array = np.zeros((0, Trafic22Array.shape[1] + 1))
    sum22 = get_sum(Trafic22Array).astype(float)

    for c, items in enumerate(Trafic22Array[:-1]):
        tr_data = items.copy()
        traf_name = tr_data[0]

        traf_sum = f"{(tr_data[-1:][0].astype(float)/sum22):.2f}"  # для экспорта в эксель (удалено умножить на 100)
        # traf_sum = f"{(tr_data[-1:][0].astype(float)/sum22):.1%}" #для корректного отображения в массиве

        traf_data = tr_data[1:-1].astype(float).tolist()
        d_data = get_derev(traf_data)
        d_sum = sum(d_data)
        d_name = "d_" + items[0].copy()  # добавляем d к строчке с названием исчтоника

        row1 = [0.0, traf_sum] + reduce_nums(traf_data, 1) + [traf_name]
        row2 = [0.0, cut_num2(d_sum), 0.0] + reduce_nums(d_data, 2) + [d_name]

        Period_Details_Array = np.vstack([Period_Details_Array, row1])
        Period_Details_Array = np.vstack([Period_Details_Array, row2])

    return Period_Details_Array


def get_details_array2(Trafic22Array):
    Period_Details_Array = np.zeros((0, Trafic22Array.shape[1]))
    for c, items in enumerate(Trafic22Array[:-1]):
        tr_data = items.copy()
        traf_name = tr_data[0]
        traf_sum = tr_data[-1:][0].astype(float)
        traf_data = tr_data[1:-1].astype(float).tolist()
        d_data = get_derev(traf_data)
        d_sum = sum(d_data)
        d_name = "d_" + items[0].copy()  # добавляем d к строчке с названием исчтоника

        row1 = [traf_name, cut_num1(traf_sum)] + reduce_nums(traf_data, 1)
        row2 = [d_name, cut_num2(d_sum), 0.0] + reduce_nums(d_data, 2)

        Period_Details_Array = np.vstack([Period_Details_Array, row1])
        Period_Details_Array = np.vstack([Period_Details_Array, row2])

    return Period_Details_Array


def join_headlines(table_head, Period_Details_Array, HeadlinesArray):
    # Сколько пустых строк нужно добавить
    # Из общей длинны таблицы вычитаем длинну детализации источников и таблицу заголовков
    empty_rows = len(table_head) - (
        Period_Details_Array.shape[1] + HeadlinesArray.shape[1]
    )
    # высота наращиваемой таблицы
    empty_h = Period_Details_Array.shape[0] - HeadlinesArray.shape[0]
    B = np.zeros((HeadlinesArray.shape[0], empty_rows))
    HeadlinesArray = np.hstack([B, HeadlinesArray])
    B = np.zeros((empty_h, HeadlinesArray.shape[1]))
    HeadlinesArray = np.vstack([HeadlinesArray, B])
    Period_Details_Array = np.hstack([HeadlinesArray, Period_Details_Array])
    return HeadlinesArray


def add_positions(A, num):
    A = A[A[:, num].astype(float).argsort()[::-1]]
    positions = np.arange(1, A.shape[0] + 1)
    A = np.hstack([A, positions.reshape(-1, 1)])
    return A


def get_headlines_array(counter_n, date1, date2, num_hl):
    headlines_data22 = get_headlines_data(counter_n, date1, date2, num_hl)
    HeadlinesArray22 = hl_arr(headlines_data22)
    return HeadlinesArray22


def get_mainline(Trafic22Array, site=2021, dif=0):
    sum22 = get_sum(Trafic22Array).astype(float)

    mainline22 = Trafic22Array[Trafic22Array.shape[0] - 1][1:-1].astype(float).tolist()

    d_mainline22 = get_derev(mainline22)
    d_sum22 = sum(d_mainline22)

    mainline22 = reduce_nums(mainline22, 1)
    d_mainline22 = reduce_nums(d_mainline22, 2)
    main_row1 = (
        [0.0, 0.0, site, cut_num1(sum22), cut_num1(dif), cut_num2(d_sum22)]
        + mainline22
        + [0.0]
    )  # пропущены позиции в рейтинге

    main_row2 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0] + d_mainline22 + [0.0]

    return main_row1, main_row2


def one_media_array(site, counter_n, date1, date2, date11, date22):
    HeadlinesArray22 = get_headlines_array(
        counter_n, date1, date2, 7
    )  # сколько заголовков выгружать
    HeadlinesArray21 = get_headlines_array(counter_n, date11, date22, 7)

    traf22 = get_trafic_data(counter_n, date1, date2)
    traf21 = get_trafic_data(counter_n, date11, date22)
    Trafic22Array = get_arrays(traf22)
    Trafic21Array = get_arrays(traf21)

    periods_for_head = list()
    for item in traf22["time_intervals"]:
        periods_for_head.append(f"{item[0]} - {item[1]}")

    table_head = (
        [
            "#",
            "d#",
            "Издание",
            "Всего визитов",
            "К прошлому периоду",
            "Динамика периода",
        ]
        + periods_for_head
        + ["источники"]
    )

    sum22 = get_sum(Trafic22Array).astype(float)
    sum21 = get_sum(Trafic21Array).astype(float)

    dif = sum22 - sum21

    mainline22, d_mainline22 = get_mainline(Trafic22Array, site, dif)
    mainline21, d_mainline21 = get_mainline(Trafic21Array)

    # собираем детали за этот год
    Period_Details_Array22 = get_details_array(Trafic22Array)
    HeadlinesArray22 = join_headlines(
        table_head, Period_Details_Array22, HeadlinesArray22
    )
    Period_Details_Array22 = np.hstack([HeadlinesArray22, Period_Details_Array22])

    # формируем общую таблицу
    One_Media_Array = np.zeros((0, len(table_head)))
    One_Media_Array = np.vstack([One_Media_Array, mainline22])
    One_Media_Array = np.vstack([One_Media_Array, d_mainline22])
    One_Media_Array = np.vstack([One_Media_Array, Period_Details_Array22])

    # собираем детали за прошлый год
    Period_Details_Array21 = get_details_array(Trafic21Array)
    HeadlinesArray21 = join_headlines(
        table_head, Period_Details_Array21, HeadlinesArray21
    )
    Period_Details_Array21 = np.hstack([HeadlinesArray21, Period_Details_Array21])
    # добавляем в общую таблицу
    empty_row = np.zeros((1, One_Media_Array.shape[1]))
    One_Media_Array = np.vstack([One_Media_Array, empty_row])

    One_Media_Array = np.vstack([One_Media_Array, mainline21])
    One_Media_Array = np.vstack([One_Media_Array, d_mainline21])
    One_Media_Array = np.vstack([One_Media_Array, Period_Details_Array21])

    return sum22, sum21, One_Media_Array, table_head
