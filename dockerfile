FROM python:3.9.10

WORKDIR /app

COPY Data_Ingestions.py Data_Ingestions.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt 

ENTRYPOINT [ "python", "Data_Ingestions.py" ]