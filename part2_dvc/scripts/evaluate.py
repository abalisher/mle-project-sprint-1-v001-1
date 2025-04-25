import pandas as pd
from sklearn.model_selection import KFold, cross_validate
import yaml
import os
import json
import joblib

def evaluate_model():
    # Чтение параметров
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # Загрузка модели
    with open('models/fitted_model.pkl', 'rb') as fd:
        pipeline = joblib.load(fd)

    # Загрузка и подготовка данных
    data = pd.read_csv('data/initial_data.csv')
    data = data.sample(n=1000, random_state=42)

    y = data[params['target_col']]
    X = data.drop(columns=params['target_col'])

    # Кросс-валидация
    cv_strategy = KFold(n_splits=params['n_splits'], shuffle=True, random_state=42)
    cv_res = cross_validate(
        pipeline,
        X,
        y,
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['metrics']
    )

    # Усреднение метрик
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3)

    # Сохранение результатов
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as fd:
        json.dump(cv_res, fd, indent=4)

if __name__ == '__main__':
    evaluate_model()