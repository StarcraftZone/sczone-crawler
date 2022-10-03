FROM python:3

WORKDIR /app

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY . /app

ENV REGION_NO=5

CMD ["sh", "-c", "python -u main.py $REGION_NO"]
