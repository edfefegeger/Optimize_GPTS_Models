import openai
import csv
import time
import multiprocessing
from multiprocessing import Value


input_file = "input.txt"
output_template = "output_{}.csv"
keys = [
    "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", "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",
    "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", "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",
    "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", "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", "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"
]
waiterror = 60
showdebug = False

def worker(api_key, lines, output_file, processed_questions_file):
    w_file = open(output_file, mode="w", encoding='utf-8')
    file_writer = csv.writer(w_file, delimiter=",", quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    file_writer.writerow(["Вопрос", "Ответ"])

    # Чтение номеров уже обработанных вопросов
    try:
        with open(processed_questions_file, 'r') as f:
            processed_questions = set(map(int, f.read().strip().split(',')))
    except FileNotFoundError:
        # Если файл не существует, начинаем с пустого множества
        processed_questions = set()

    for i, line in enumerate(lines):
        # Если вопрос уже был обработан, пропустить его
        if i in processed_questions:
            continue

        print("Вопрос:", line)
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
                    {"role": "user", "content": line}
                ]
            )
            if showdebug:
                print(completion)
            if "choices" in completion and len(completion.choices) > 0 and "message" in completion.choices[0]:
                file_writer.writerow([line, completion.choices[0].message["content"]])
                w_file.flush()
                # Сохранение номера обработанного вопроса в processed_questions
                processed_questions.add(i)
                # Сохранение номеров обработанных вопросов в файл
                with open(processed_questions_file, 'w') as f:
                    f.write(','.join(map(str, processed_questions)))
            else:
                print("Ошибка")
        except Exception as e:
            print(e)
            if "exceeded your current quota" in str(e):
                print("Лимит. Берем следующий ключ", api_key)
                break
            else:
                time.sleep(waiterror)
    w_file.close()

if __name__ == "__main__":
    with open(input_file, 'r', encoding='UTF-8') as file:
        lines = [line.rstrip() for line in file]

    processes = []
    for i, (api_key) in enumerate(keys):
        output_file = output_template.format(i)
        processed_questions_file = f"processed_questions_{i}.txt"
        p = multiprocessing.Process(target=worker, args=(api_key, lines, output_file, processed_questions_file))
        processes.append(p)
        p.start()

    for process in processes:
        process.join()
