import os
from data_processing.rep_config import ReportConfig
from data_processing.csv_woker import DataWorker
from pathlib import Path
from data_processing.utils import (
    clean_names,
    get_dir_content
)
import pandas as pd


def reshape1(path_in='data/xls', path_out='data/csv'):
    path_in = Path(path_in)
    path_out = Path(path_out)

    # files = ['2023_w50_clean.xlsx']
    files = get_dir_content(path_in, 'xlsx')
    for file in files:
        print(f'reading file {file}')
        data_file = Path(file)
        data_xls = pd.read_excel(data_file, sheet_name=0, dtype=str, index_col=None)

        suffix = '.csv.gz'
        func = lambda x: "4+" in x or 'Sales' not in x

        value_vars = list(filter(func, data_xls.columns[17:50]))
        # print(value_vars)
        data_xls = pd.melt(data_xls,
                           id_vars=data_xls.columns[:17],
                           value_vars=value_vars,
                           var_name='type',
                           value_name='value')
        data_xls[['TRP type', 'TA']] = data_xls.pop('type').str.split(r'(?= Все)', expand=True).apply(
            lambda x: [e.strip() for e in x])
        out_file = Path(path_out, data_file.name).with_suffix('.xlsx')
        # data_xls.to_excel(out_file, sheet_name='Sheet1', na_rep='', float_format=None, columns=None,
        #                   header=True, index=False)
        out_file = Path(path_out, data_file.name).with_suffix('.csv.gz')
        data_xls.to_csv(path_or_buf=out_file, mode='x', decimal=',', sep=';', encoding='utf-8', index=False)


def reshape2(path_in='data/xls', path_out='data/csv'):
    path_in = Path(path_in)
    path_out = Path(path_out)

    # files = ['2023_w50_clean.xlsx']
    files = get_dir_content(path_in, 'xlsx')
    for file in files:
        print(f'reading file {file}')
        data_file = Path(file)

        data_xls = pd.read_excel(data_file, sheet_name=0, na_filter=False, index_col=None, na_values="",
                                 )
        data_xls = data_xls[
            ['Год',
             'Начало месяца',
             'Начало недели',
             'День',
             'Категория',
             'Фокус ЦА',
             'Бренд',
             'Продукт',
             'Ролик ожидаемой длителности',
             'Ролик',
             'Ролик id',
             'Ролик тип',
             'Ролик распространение',
             'Прайм/офф-прайм',
             'Национальная телекомпания',
             'Программа',
             'Программа тип',
             'Quantity Все 4+',
             'Sales TVR Все 4+',
             'TVR Все 4+',
             'Stand. TVR (20) Все 4+',
             'TVR Все 18+',
             'Stand. TVR (20) Все 18+',
             'TVR Все 30+ (Итого)',
             'Stand. TVR (20) Все 30+ (Итого)',
             'TVR Все 30..60 BC (Кредит)',
             'Stand. TVR (20) Все 30..60 BC (Кредит)',
             'TVR Все 30..50 AB (КК и КР)',
             'Stand. TVR (20) Все 30..50 AB (КК и КР)',
             'TVR Все 45..60 BC (Вклады)',
             'Stand. TVR (20) Все 45..60 BC (Вклады)',
             'TVR Все 55+ AB (Пенс)',
             'Stand. TVR (20) Все 55+ AB (Пенс)',
             'TVR Все 30..60 С (Бизнес)',
             'Stand. TVR (20) Все 30..60 С (Бизнес)'
             ]
        ]
        for d in ['Начало месяца', 'Начало недели', 'День']:
            data_xls[d] = pd.to_datetime(data_xls[d])

        out_file = Path(path_out, data_file.name).with_suffix('.xlsx')
        writer = pd.ExcelWriter(out_file,
                                engine='xlsxwriter',
                                datetime_format='dd.mm.yyyy',
                                date_format='dd.mm.yyyy')
        data_xls.to_excel(writer, sheet_name='Sheet1', na_rep='', float_format=None, columns=None,
                          header=True, index=False)
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]

        # Get the dimensions of the dataframe.
        (max_row, max_col) = data_xls.shape
        worksheet.set_column(1, max_col, 20)

        writer.close()


def reshape3(path_in='data/raw', path_out='data/clean'):
    path_in = Path(path_in)
    path_out = Path(path_out)

    # files = ['2023_w50_clean.xlsx']
    files = get_dir_content(path_in, 'xlsx')
    for file in files:
        print(f'reading file {file}')
        data_file = Path(file)

        data_xls = pd.read_excel(data_file,
                                 sheet_name=0,
                                 na_filter=False,
                                 index_col=None,
                                 na_values="",
                                 )
        data_xls = data_xls[
            ['Год',
             'Начало месяца',
             'Начало недели',
             'День',
             'Категория',
             'Фокус ЦА',
             'Бренд',
             'Продукт',
             'Ролик ожидаемой длителности',
             'Ролик',
             'Ролик id',
             'Ролик тип',
             'Ролик распространение',
             'Прайм/офф-прайм',
             'Национальная телекомпания',
             'Программа',
             'Программа тип',
             'Quantity Все 4+',
             'Sales TVR Все 4+',
             'TVR Все 4+',
             'Stand. TVR (20) Все 4+',
             'TVR Все 18+',
             'Stand. TVR (20) Все 18+',
             'TVR Все 30+ (Итого)',
             'Stand. TVR (20) Все 30+ (Итого)',
             'TVR Все 30..60 BC (Кредит)',
             'Stand. TVR (20) Все 30..60 BC (Кредит)',
             'TVR Все 30..50 AB (КК и КР)',
             'Stand. TVR (20) Все 30..50 AB (КК и КР)',
             'TVR Все 45..60 BC (Вклады)',
             'Stand. TVR (20) Все 45..60 BC (Вклады)',
             'TVR Все 55+ AB (Пенс)',
             'Stand. TVR (20) Все 55+ AB (Пенс)',
             'TVR Все 30..60 С (Бизнес)',
             'Stand. TVR (20) Все 30..60 С (Бизнес)'
             ]
        ]
        for d in ['Начало месяца', 'Начало недели', 'День']:
            data_xls[d] = pd.to_datetime(data_xls[d])
        data_xls['Бренд'] = data_xls['Бренд'].replace('ТИНЬКОФФ', 'Т-БАНК', regex=True)

        out_file = Path(path_out, data_file.name).with_suffix('.xlsx')
        writer = pd.ExcelWriter(out_file,
                                engine='xlsxwriter',
                                datetime_format='dd.mm.yyyy',
                                date_format='dd.mm.yyyy')
        data_xls.to_excel(writer, sheet_name='Sheet1', na_rep='', float_format=None, columns=None,
                          header=True, index=False)
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]

        # Get the dimensions of the dataframe.
        (max_row, max_col) = data_xls.shape
        worksheet.set_column(1, max_col, 20)

        writer.close()


if __name__ == '__main__':
    reshape3()
