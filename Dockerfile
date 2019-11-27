FROM python:3
WORKDIR /app
COPY . /app
RUN apt-get update && apt-get install -y cron curl
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 13800
ENV ADDRESS EMPTY
ENV VIEW EMPTY
ENV REPL_FACTOR EMPTY
RUN chmod -v +x start.sh
CMD ./start.sh && python main.py $ADDRESS $VIEW $REPL_FACTOR