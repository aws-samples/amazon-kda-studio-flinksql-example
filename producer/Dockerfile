FROM public.ecr.aws/lambda/python:3.8.2021.08.23.12
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY main.py main.py
CMD ["python3", "main.py"]