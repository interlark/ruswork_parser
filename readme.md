# Парсер rus-work.com

## Установка

```bash
git clone https://github.com/interlark/ruswork_parser
cd ruswork_parser
pip3 install -r requirements.txt
```

## Использование

```bash
python3 parser.py ижевск izhevsk_result.csv
```

```text
target_type = город
target_place = ижевск
output_path = izhevsk_result.csv
output_encoding = utf-8
n_parallel = 10
Запуск парсера по названию города: ижевск
Парсинг:  28%|███████▍                   | 1022/3702 [12:26<32:42,  1.37стр./s]
```
