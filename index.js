const aws = require("aws-sdk");
const fs = require('fs');
const https = require('https');
const { exec } = require('child_process');

const REQUEST_QUEUE_URL = process.env.QUEUE_URL;

const params = {
  QueueUrl: REQUEST_QUEUE_URL,
  MaxNumberOfMessages: 1,
  VisibilityTimeout: 60,
};

const sqs = new aws.SQS({ region: "us-west-2" });

const setImage = (userId, image) => new Promise((resolve, reject) => {
    const ddb = new aws.DynamoDB({
        region: 'us-west-2'
    });

    const attributes = {
        'image': {
            Action: 'PUT',
            Value: {
                S: image 
            }
        }
    }

    const updateParams = {
        TableName: 'developer',
        Key: {
            'developer_id': {
                S: userId
            }
        },
            AttributeUpdates: attributes
        };

        ddb.updateItem(updateParams, (err, putResult) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });

});

const downloadAsset = (assetId) => new Promise((resolve, reject) => {
    const outPath = '/Users/josephgarcia/nsfw_model/assets/' + assetId;
    const writeStream = fs.createWriteStream(outPath);

    writeStream.on('close', () => {
        resolve(outPath);
    });

    https.get(`https://assets.homegames.io/${assetId}`, (res) => {
        res.pipe(writeStream);
    });
});

const handlePublishEvent = ({ userId, assetId }) => new Promise((resolve, reject) => {
    downloadAsset(assetId).then(assetPath => {
        exec(`bash run.sh ${assetPath}`, (err, stdout, stderr) => {
            console.log('got this here cool');
            console.log(err);
            console.log(stderr);
            console.log(stdout);
            if (stdout.trim() === 'fail') {
                console.log("failed - image nsfw");
            } else if (stdout.trim() === 'success') {
                console.log('gonna set image');
                setImage(userId, assetId);
            }
        });
    });
});

sqs.receiveMessage(params, (err, data) => {
  try {
    if (data && data.Messages) {
      const request = JSON.parse(data.Messages[0].Body);
      console.log(request);
      handlePublishEvent(request);
      const deleteParams = {
        QueueUrl: params.QueueUrl,
        ReceiptHandle: data.Messages[0].ReceiptHandle,
      };

      sqs.deleteMessage(deleteParams, (err, data) => {
        console.log(err);
        console.log(data);
        console.log("deleted");
      });
    }
  } catch (e) {
    console.log("error processing message");
    console.log(e);
  }
});

