import os
from dotenv import load_dotenv
from airflow.providers.telegram.hooks.telegram import TelegramHook

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram_message(text: str):
    hook = TelegramHook(
        token=TELEGRAM_TOKEN,
        chat_id=TELEGRAM_CHAT_ID
    )
    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text
    })

def send_telegram_failure_message(context):
    run_id = context.get('run_id')
    task_instance_key = context.get('task_instance_key_str')
    message = f'❌ Ошибка при выполнении DAG!\nRun ID: {run_id}\nЗадача: {task_instance_key}'
    send_telegram_message(message)

def send_telegram_success_message(context):
    dag_id = context['dag'].dag_id
    run_id = context.get('run_id')
    message = f'✅ DAG успешно завершён!\nDAG ID: {dag_id}\nRun ID: {run_id}'
    send_telegram_message(message)