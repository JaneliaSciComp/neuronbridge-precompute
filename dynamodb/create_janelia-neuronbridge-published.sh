aws dynamodb create-table \
    --table-name janelia-neuronbridge-published\
    --attribute-definitions AttributeName=key,AttributeType=S AttributeName=keyType,AttributeType=S \
    --key-schema AttributeName=key,KeyType=HASH AttributeName=keyType,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=200,WriteCapacityUnits=200 \
    --tags Key=PROJECT,Value=NeuronBridge Key=DEVELOPER,Value=svirskasr Key=STAGE,Value=prod
