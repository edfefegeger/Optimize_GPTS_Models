import openai
import csv
import time
import multiprocessing
from multiprocessing import Lock, Value

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
number_of_keys = len(keys)
waiterror = 30
showdebug = False
processed_questions_file = "processed_questions.txt"
file_lock = Lock()
current_line_number = Value('i', 0)
files_processed = Value('i', 0)

def worker(api_key, questions, output_file, current_line_number):
    w_file = open(output_file, mode="a", encoding='utf-8')
    file_writer = csv.writer(w_file, delimiter=",", quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    file_writer.writerow(["Вопрос", "Ответ"])
    for i, question in enumerate(questions, start=current_line_number.value):
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

                # Увеличиваем текущий номер обрабатываемой строки и записываем его в файл
                with current_line_number.get_lock(), file_lock:
                    current_line_number.value += 1
                    with open(processed_questions_file, 'w') as f:
                        f.write(str(current_line_number.value))

                with file_lock:
                    files_processed.value += 1
            else:
                print("Ошибка")
        except Exception as e:
            print(e)
            if "exceeded your current quota" in str(e):
                print("Лимит. Берем следующий ключ", api_key)
                break
            else:
                time.sleep(waiterror)

if __name__ == "__main__":
    with open(input_file, 'r', encoding='UTF-8') as file:
        lines = [line.rstrip() for line in file]

    # Получение номера строки, с которой следует начать обработку
    try:
        with open(processed_questions_file, 'r') as f:
            saved_line_number = int(f.read().strip())
            current_line_number.value = saved_line_number
            print(f"Начинаем с обработки с позиции {saved_line_number}")
    except FileNotFoundError:
        pass

    # Разделение запросов на наборы для каждого ключа
    data_splits = [[] for _ in range(len(keys))]
    for i, line in enumerate(lines[current_line_number.value:], start=current_line_number.value):
        data_splits[i % len(keys)].append(line)

    processes = []
    for i, (api_key, questions) in enumerate(zip(keys, data_splits)):
        output_file = output_template.format(i)
        p = multiprocessing.Process(target=worker, args=(api_key, questions, output_file, current_line_number))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()

    print(f"Обработано {files_processed.value} файлов.")
    
    # Сохранение текущей позиции в processed_questions.txt
    with open(processed_questions_file, 'w') as f:
        f.write(str(current_line_number.value))