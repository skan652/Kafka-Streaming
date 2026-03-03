import os, csv, json


def convert(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f_in, open(output_path, 'w', encoding='utf-8') as f_out:
        reader = csv.DictReader(f_in)
        for row in reader:
            f_out.write(json.dumps(row) + '\n')


if __name__ == '__main__':
    base = os.path.dirname(__file__)
    clean_in = os.path.abspath(os.path.join(base, '..', 'csv_lab', 'data', 'transactions.csv'))
    dirty_in = os.path.abspath(os.path.join(base, '..', 'csv_lab', 'data', 'transactions_dirty.csv'))
    clean_out = os.path.abspath(os.path.join(base, 'data', 'transactions.jsonl'))
    dirty_out = os.path.abspath(os.path.join(base, 'data', 'transactions_dirty.jsonl'))

    convert(clean_in, clean_out)
    convert(dirty_in, dirty_out)
    print('Done conversions')
