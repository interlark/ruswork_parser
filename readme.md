# Парсер rus-work.com

## Установка

```bash
git clone https://github.com/interlark/ruswork_parser
cd ruswork_parser
pip3 install -r requirements.txt
```

## Использование
1. Парсер можно запустить на определенный город.

```bash
python3 parser.py город ижевск izhevsk_result.csv
```

```text
target_type = город
target_place = ижевск
output_path = izhevsk_result.csv
output_encoding = utf-8
n_parallel = 10
Запуск парсера по городу ижевск (1 URL-адресов)
Парсинг:  28%|███████▍                   | 1022/3702 [12:26<32:42,  1.37стр./s]
```

2. Парсер можно запустить на целый [регион](https://rus-work.com/regions.html).

```bash
python3 parser.py регион архангельск arkhangelsk_dist_result.csv
```

```text
target_type = регион
target_place = архангельск
output_path = arkhangelsk_dist_result.csv
output_encoding = utf-8
n_parallel = 10
Запуск парсера по региону архангельск (13 URL-адресов)
Парсинг:  12%|███▍                        | 242/1978 [02:46<19:58,  1.45стр./s]
```

3. Можно спарсить всё.

```bash
python3 parser.py all all result.csv
```

```text
target_type = all
target_place = all
output_path = result.csv
output_encoding = utf-8
n_parallel = 10
Запуск парсера по всем городам (1132 URL-адресов)
Парсинг:  24%|███████                      | 157/646 [01:48<05:47,  1.41стр./s]
```
