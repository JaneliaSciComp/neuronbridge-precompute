if [ $@ ]
then
  VERSION=$1
else
  echo "You must specify a version"
  exit
fi
echo "Creating janelia-neuronbridge-published-${VERSION}"
aws dynamodb create-table \
    --table-name janelia-neuronbridge-published-${VERSION}\
    --attribute-definitions AttributeName=itemType,AttributeType=S AttributeName=searchKey,AttributeType=S \
    --key-schema AttributeName=itemType,KeyType=HASH AttributeName=searchKey,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=PROJECT,Value=NeuronBridge Key=DEVELOPER,Value=svirskasr Key=STAGE,Value=prod

# Formerly used PROVISIONED mode
#--provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=50 \
