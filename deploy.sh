docker build -t python_docker .
docker tag python_docker:latest 381014012748.dkr.ecr.us-west-1.amazonaws.com/python_docker:latest
aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 381014012748.dkr.ecr.us-west-1.amazonaws.com
docker push 381014012748.dkr.ecr.us-west-1.amazonaws.com/python_docker:latest
docker inspect pyton_docker | grep Cmd
aws lambda update-function-code --function-name PythonDocker \
  --image-uri 381014012748.dkr.ecr.us-west-1.amazonaws.com/python_docker:latest

