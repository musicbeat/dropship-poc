console.log('Loading function');
var aws = require('aws-sdk');
var s3 = new aws.S3();
var sns = new aws.SNS();
var dynamodb = new aws.DynamoDB();

exports.handler = function(event, context) {
    console.log("Received event ", JSON.stringify(event, null, 2));

    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    var eTag = event.Records[0].s3.object.eTag;

    // Insert into the dynamodb table, documents-received, using the etag as the
    // primary key. If the insert fails, a file with an identical hash has already
    // been processed, so reject the duplicate. If the insert succeeds, then the
    // etag is recorded so that subsequent duplicates will be detected.
    var tableName = "documents-received";
    var receivedDateTime = new Date().toJSON();
    var receivedItem = {
        "etag": {"S": eTag},
        "receivedDateTime": {"S": receivedDateTime},
        "fileName": {"S": key}
    };
    dynamodb.putItem({
        "TableName": tableName,
        "ConditionExpression":  "etag <> :e",
        "ExpressionAttributeValues": {
            ":e": {"S": eTag}
        },
        "Item": receivedItem
    }, function(err, data) {
        if (err) {
            console.log('putting item into dynamodb failed: ' + err);
            // If there was a duplicate, then (1) retrieve the matched data from
            // dynamodb; (2) publish to SNS; (3) copy the file to the inerror bucket;
            // (4) delete the file from the indigestion bucket.
            dynamodb.getItem({
                "TableName": tableName,
                "Key": {
                    "etag": {"S": eTag}
                },
                "ProjectionExpression": "etag, receivedDateTime, fileName"
            }, function(err, data) {
                if (err) {
                    console.log("error reading data", err);
                } else {
                    console.log("got item data", JSON.stringify(data, null, 2));
                    var existingFileName = data.Item.fileName.S;
                    console.log("existingFileName: ", existingFileName);
                    var payload = "Uploaded file " + key + " matches existing file " + existingFileName + "\n" + JSON.stringify(data, null, 2);
                    var snsParams = {
                        Message: payload,
                        Subject: 'Duplicate file received',
                        TopicArn: 'arn:aws:sns:us-west-2:134529390395:digester-alarm'
                    };
                    console.log(snsParams);
                    sns.publish(snsParams, function(err, data) {
                        if (err) {
                            console.log("Error sending SNS ", err);
                        } else {
                            console.log("published to sns.");
                            var copyParams = {
                                Bucket: 'inerror',
                                CopySource: 'indigestion' + '/' + key,
                                Key: key
                            };
                            s3.copyObject(copyParams, function(err, data) {
                                if (err) console.log(err, err.stack); // an error occurred
                                else {
                                    console.log("Copied duplicate file to inerror");
                                    // now delete the file from indigestion
                                    var deleteParams = {
                                        Bucket: bucket,
                                        Key: key
                                    };
                                    console.log(deleteParams);
                                    s3.deleteObject(deleteParams, function(err, data) {
                                        if (err) console.log(err, err.stack); // an error occurred
                                        else {
                                            console.log("Deleted file from indigestion");           // successful response
                                            context.succeed();
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        } else {
            // otherwise, copy the file to inprogress bucket then delete it from indigestion
            console.log("new file, so copy to inprogress");
            var copyParams = {
                Bucket: 'inprogress',
                CopySource: 'indigestion' + '/' + key,
                Key: key
            };
            s3.copyObject(copyParams, function(err, data) {
                if (err) {
                    console.log(err, err.stack); // an error occurred
                } else {
                    console.log("copied to inprogress");  // successful response
                    // now delete the file from indigestion
                    var deleteParams = {
                        Bucket: bucket,
                        Key: key
                    };
                    s3.deleteObject(deleteParams, function(err, data) {
                        if (err) console.log(err, err.stack); // an error occurred
                        else {
                            console.log("deleted file from indigestion");           // successful response
                            context.succeed();
                        }
                    });
                }
            });
        }
    });
};
