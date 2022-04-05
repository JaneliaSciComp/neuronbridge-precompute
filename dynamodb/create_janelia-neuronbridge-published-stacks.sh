echo "Creating janelia-neuronbridge-published-stacks"
aws dynamodb create-table \
    --table-name janelia-neuronbridge-published-stacks\
    --attribute-definitions AttributeName=itemType,AttributeType=S AttributeName=searchKey,AttributeType=S \
    --key-schema AttributeName=itemType,KeyType=HASH AttributeName=searchKey,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=PROJECT,Value=NeuronBridge Key=DEVELOPER,Value=svirskasr Key=STAGE,Value=prod
