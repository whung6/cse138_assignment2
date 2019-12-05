FROM python:3-slim
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 13800
ENV ADDRESS EMPTY
ENV VIEW EMPTY
ENV REPL_FACTOR EMPTY
ENV PYTHONHASHSEED 0
CMD python main.py $ADDRESS $VIEW $REPL_FACTOR