FROM python:2.7-slim
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 13800
ENV ADDRESS EMPTY
ENV VIEW EMPTY
CMD python main.py $ADDRESS $VIEW