FROM python:3-slim
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 13800
ENV ADDRESS "127.0.0.1"
ENV VIEW "127.0.0.1"
ENV REPL_FACTOR 1
ENV PYTHONHASHSEED 0
CMD python main.py $ADDRESS $VIEW $REPL_FACTOR
