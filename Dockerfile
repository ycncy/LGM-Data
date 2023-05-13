FROM python:3.10

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

cmd ["python", "add_data_until_last_record.py"]