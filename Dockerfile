FROM python:3.11

# Install R and system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    r-base \
    r-base-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    build-essential

# Install Python requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install R packages
RUN R -e "install.packages(c('fitzRoy'), repos='https://cloud.r-project.org')"

# Copy app code
COPY . /app
WORKDIR /app

CMD ["python", "main.py"]
