echo "Deleting janelia-neuronbridge-publishing-doi"
aws dynamodb delete-table --table-name janelia-neuronbridge-publishing-doi
sleep 5
echo "Creating janelia-neuronbridge-publishing-doi"
aws dynamodb create-table \
    --table-name janelia-neuronbridge-publishing-doi\
    --attribute-definitions AttributeName=name,AttributeType=S \
    --key-schema AttributeName=name,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=PROJECT,Value=NeuronBridge Key=DEVELOPER,Value=svirskasr Key=STAGE,Value=prod
