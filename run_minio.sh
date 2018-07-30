docker run -d -p 9000:9000 --name minio \
      --env MINIO_ACCESS_KEY=${AWS_ACCESS_KEY_ID} \
      --env MINIO_SECRET_KEY=${AWS_SECRET_ACCESS_KEY} \
      -v ${MINIO_DATA}:/export \
      -v ${MINIO_CONFIG}:/root/.minio \
      minio/minio server
