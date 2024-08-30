import json
import os

def split_json_file(input_file, output_dir, num_files):
    with open(input_file, 'r') as file:
        data = json.load(file)
        print(len(data))

    chunk_size = len(data) // num_files
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    for i, chunk in enumerate(chunks):
        output_file = os.path.join(output_dir, f'output_{i+1}.json')
        with open(output_file, 'w') as file:
            json.dump(chunk, file, indent=4)

        print(f'Создан файл: {output_file}')

if __name__ == '__main__':
    input_file = '/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/readFiles/nodesCoin.json'
    output_dir = '/Users/assistentka_professora/Desktop/ArangoDB/arangoLoadData/splitData/coinNode'
    num_files = 3
    # with open(input_file, 'r') as file:
    #     data = json.load(file)
    #     print(len(data))
    split_json_file(input_file, output_dir, num_files)


