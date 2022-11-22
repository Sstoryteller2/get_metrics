#!/usr/bin/env python3
# coding: utf-8

from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import numpy as np
import config
import json
import io
import utils_f
from pathlib import Path


def main():
    with open("counters.json", "r", newline=None) as f:
        counters = f.read()
        counters = json.loads(counters)
    # Формат даты YYYY-MM-DD
    date1 = config.date1
    date2 = config.date2

    date11 = config.date11
    date22 = config.date22

    all_arrays_dict = dict()
    i = 0
    filename = ""
    for k, v in counters.items():
        len(counters.items())
        if 2 > i or i > len(counters.items()) - 3:
            filename += (k.replace("www.", "").replace(".ru", ""))[:2] + "_"
        sum22, sum21, One_Media_Array, table_head = utils_f.one_media_array(
            k, v, date1, date2, date11, date22
        )
        all_arrays_dict[k] = [sum22, sum21, One_Media_Array]
        print("Получены данные ", k, v)
        i += 1
    filename += datetime.now().strftime("%d-%m-%y")

    # Название, трафик этот год, трафик прошлый год, позиция этот год, позиция прошлый год, разница
    # собираем сортировку
    A = np.zeros((0, 3))

    for key, value in all_arrays_dict.items():
        A = np.vstack([A, [key, value[0], value[1]]])

    A = utils_f.add_positions(A, 2)  # прошлый год
    A = utils_f.add_positions(A, 1)  # этот год
    B = A[:, 3].astype(int) - A[:, 4].astype(int)
    A = np.hstack([A, B.reshape(-1, 1)])
    # вносим данные в массивы
    for item in A:
        all_arrays_dict[item[0]][2][0][0] = item[3]  # позиция этом году
        all_arrays_dict[item[0]][2][0][1] = item[5]  # изменение позиции
        shape = all_arrays_dict[item[0]][2].shape

    emty_row = np.zeros((2, shape[1]))
    ArrayToPublish = np.zeros((0, shape[1]))
    for item in A[:, 0]:
        ArrayToPublish = np.vstack([ArrayToPublish, all_arrays_dict[item][2]])
        ArrayToPublish = np.vstack([ArrayToPublish, emty_row])

    df = pd.DataFrame(ArrayToPublish, columns=table_head)
    df[df.columns[3:-1]] = df[df.columns[3:-1]].apply(lambda x: x.astype("float64"))
    reports = Path("reports", filename + ".xlsx")
    print(f"Результат сохранён в файле {reports}")
    df.to_excel(reports, sheet_name="all_branches", index=False)


if __name__ == "__main__":
    main()
