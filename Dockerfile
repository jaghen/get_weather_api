FROM python:3.7-slim

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install dependencies.
RUN pip install -r requirements.txt
#contained by your image, along with any arguments.

CMD [ "python", "./test.py"]
