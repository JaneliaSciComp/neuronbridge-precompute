aws dynamodb create-table \
    --table-name janelia-neuronbridge-published-dev\
    --attribute-definitions AttributeName=itemType,AttributeType=S AttributeName=searchKey,AttributeType=S \
    --key-schema AttributeName=itemType,KeyType=HASH AttributeName=searchKey,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=50 \
    --tags Key=PROJECT,Value=NeuronBridge Key=DEVELOPER,Value=svirskasr Key=STAGE,Value=dev
