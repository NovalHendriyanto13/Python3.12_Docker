# Use Python 3 base image
FROM python:3.12

# Set working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl gnupg2 unixodbc unixodbc-dev gcc g++ \
    libpq-dev libssl-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft SQL Server ODBC Driver repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install ODBC driver
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# RUN apt-get update && apt-get install -y nodejs npm

# Copy dependencies
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Jupyter Notebook
# RUN pip install jupyter jupyterlab

# Copy project files
COPY . .

# Expose the port FastAPI runs on
EXPOSE 8080 8888

# Run FastAPI with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]
# Run Jupyter and FastAPI
# CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8080 --reload & jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root"]
