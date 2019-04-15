FROM python:3.7
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
USER root
RUN yum install file -y && yum clean all
USER 1001
CMD ["python", "./pup.py"]
