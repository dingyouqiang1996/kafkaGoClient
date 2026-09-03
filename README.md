- 运行方法:
```shell
docker run \
  -v $(pwd)/config.yml:/app/config.yml \
  -v $(pwd)/config.yml:/app/config/config.yml \
  -v $(pwd)/config.yml:/config.yml \
  --security-opt seccomp=unconfined \
  kafkagoclient:v1
```