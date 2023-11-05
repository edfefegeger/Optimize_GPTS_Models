import openai
import csv
import time
import multiprocessing
from multiprocessing import Value

input_file = "input.txt"
output_template = "output_{}.csv"
api_keys = [
    "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",
]

model_engine = "gpt-3.5-turbo"
max_tokens = 1024
wait_error = 60

def split_data(data, n):
    avg = len(data) // n
    avg = avg if avg > 0 else 1
    return [data[i:i + avg] for i in range(0, len(data), avg)]

def save_progress(question_index, section_index, output_file_index):
    with open("progress.txt", "w") as progress_file:
        progress_file.write(f"{section_index.value}:{question_index}:{output_file_index}")

def worker(api_key, lines, output_file_index, start_index=0, section_index=0):
    output_file = output_template.format(output_file_index)
    with open(output_file, mode="w", encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter=",", quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        file_writer.writerow(["Вопрос", "Ответ"])
        for i, line in enumerate(lines, start=start_index):
            if i < section_index.value:
                continue  # Пропустить вопросы до последней обработанной секции
            print("Вопрос:", line)
            try:
                openai.api_key = api_key
                response = openai.ChatCompletion.create(
                    model=model_engine,
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": line}
                    ]
                )
                
                if "choices" in response and len(response.choices) > 0 and "message" in response.choices[0]:
                    answer = response.choices[0].message['content'].strip()
                    file_writer.writerow([line, answer])
                    print("Ответ:", answer)
                else:
                    print("Ошибка в полученном ответе.")
            except Exception as e:
                print("Ошибка:", e)
                if "exceeded your current quota" in str(e):
                    print("Лимит токенов исчерпан. Переход к следующему файлу.")
                    section_index.value += 1
                    save_progress(i, section_index, output_file_index)  # Сохранить прогресс
                    return
                else:
                    time.sleep(wait_error)
            finally:
                section_index.value = i  # Обновить значение секции после каждого обработанного вопроса

if __name__ == "__main__":
    try:
        progress_data = open("progress.txt", "r").readline().strip().split(":")
        section_index = Value('i', int(progress_data[0]))
        question_index = int(progress_data[1])
        output_file_index = int(progress_data[2])
        print(f"Продолжаем с секции: {section_index.value}, вопроса: {question_index}, файл: {output_file_index}")
    except Exception as e:
        print("Ошибка при загрузке прогресса:", e)
        section_index = Value('i', 0)
        question_index = 0
        output_file_index = 0

    with open(input_file, 'r', encoding='UTF-8') as file:
        lines = [line.rstrip() for line in file]

    data_splits = split_data(lines, len(api_keys))

    processes = []
    for i, (api_key, data_split) in enumerate(zip(api_keys[section_index.value:], data_splits[section_index.value:]), start=section_index.value):
        p = multiprocessing.Process(target=worker, args=(api_key, data_split, output_file_index + i, question_index, section_index))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()
