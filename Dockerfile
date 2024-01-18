FROM python:3.9.7

WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY dagster.yml .
COPY workspace.yml .
COPY final_project ./final_project
COPY pyproject.toml .
COPY setup.cfg .
COPY setup.py .

EXPOSE 3000

CMD ["dagster-webserver", "-w", "workspace.yml", "-h", "0.0.0.0", "-p", "3000"]



