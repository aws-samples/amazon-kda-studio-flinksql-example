FROM public.ecr.aws/bitnami/python:3.9
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY main.py main.py
CMD ["python3", "main.py"]