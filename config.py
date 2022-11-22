group = "week"  # возможные периоды: day, week, month, quarter, year

# Формат даты YYYY-MM-DD
date1 = "2022-08-01"
date2 = "2022-10-30"

date11 = "2021-08-02"
date22 = "2021-10-31"

key = "ключь от метики"
api_url = "https://api-metrika.yandex.net/stat/v1/data"

header_params = {
    "GET": "/management/v1/counters HTTP/1.1",
    "Host": "api-metrika.yandex.net",
    "Authorization": "OAuth" + key,
    "Content-Type": "application/x-yametrika+json",
}
