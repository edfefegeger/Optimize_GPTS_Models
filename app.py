import openai
import csv
import time
import multiprocessing
from multiprocessing import Value

input_file = "input.txt"
output_template = "output_{}.csv"
keys = [
    "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",
    "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",
    "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",
    "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",
    "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",
    "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",
    "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"
]
waiterror = 60
showdebug = False
files_processed = multiprocessing.Value('i', 0)  # Счетчик обработанных файлов

def worker(api_key, questions, output_file):
    w_file = open(output_file, mode="w", encoding='utf-8')
    file_writer = csv.writer(w_file, delimiter=",", quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    file_writer.writerow(["Вопрос", "Ответ"])
    for question in questions:
        print("Вопрос:", question)
        try:
            openai.api_key = api_key
            completion = openai.ChatCompletion.create(
                model="gpt-3.5-turbo-0613",
                temperature=0.5,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": question}
                ]
            )
            if showdebug:
                print(completion)
            if "choices" in completion and len(completion.choices) > 0 and "message" in completion.choices[0]:
                file_writer.writerow([question, completion.choices[0].message["content"]])
                w_file.flush()
            else:
                print("Ошибка")
        except Exception as e:
            print(e)
            if "exceeded your current quota" in str(e):
                print("Лимит. Берем следующий ключ", api_key)
                break
            else:
                time.sleep(waiterror)

    with files_processed.get_lock():
        files_processed.value += 1

if __name__ == "__main__":
    with open(input_file, 'r', encoding='UTF-8') as file:
        lines = [line.rstrip() for line in file]

    # Разделение запросов на наборы для каждого ключа
    data_splits = [[] for _ in range(len(keys))]
    for i, line in enumerate(lines):
        data_splits[i % len(keys)].append(line)

    processes = []
    for i, (api_key, questions) in enumerate(zip(keys, data_splits)):
        output_file = output_template.format(i)
        p = multiprocessing.Process(target=worker, args=(api_key, questions, output_file))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()

    print(f"Обработано {files_processed.value} файлов.")
