console.log('Loading function');
var aws = require('aws-sdk');
var s3 = new aws.S3();
var sns = new aws.SNS();

exports.handler = function(event, context) {
    console.log("Received event ", JSON.stringify(event, null, 2));

    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;

    console.log("get md5 of event's object.");

    if (key.substring(0, 9) != 'accepted-') {

        // Do a HEAD request on the object from the event and get its ETag (MD5)
        s3.headObject({Bucket: bucket, Key: key}, function(err, headData) {
            console.log("checking for duplicates.");
            if (err) {
                console.log("Error getting object " + key + " from bucket " + bucket +
                ". Make sure they exist and the bucket is in the same region as this function.");
                context.fail("Error getting file: " + err);
            } else {
                var eTag = headData.ETag;
                console.log("New file's eTag: ", eTag);
                // Look at all the other objects in the S3 bucket
                s3.listObjects({Bucket: bucket}, function(err, listData) {
                    console.log(listData.Contents);
                    var match;
                    for (var file in listData.Contents) {
                        console.log("Existing file: ", listData.Contents[file].Key, "eTag: ", listData.Contents[file].ETag);
                        // Check if the file checksums match
                        if ((eTag == listData.Contents[file].ETag ) && (key != listData.Contents[file].Key)) {
                            match = file;
                            console.log("there was a match");
                            break;
                        }
                    }
                    // If there was a duplicate, then publish to SNS
                    if (match) {
                        console.log("handle the match");
                        var payload = "Uploaded file " + key + " matches existing file " + listData.Contents[file].Key;
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
                                console.log("Duplicate file received: " + listData.Contents[file].Key);
                                var copyParams = {
                                    Bucket: 'inerror',
                                    CopySource: 'indigestion' + '/' + key,
                                    Key: key
                                };
                                console.log(copyParams);
                                s3.copyObject(copyParams, function(err, data) {
                                    if (err) console.log(err, err.stack); // an error occurred
                                    else {
                                        console.log(data);           // successful response
                                        // now delete the file from indigestion
                                        var deleteParams = {
                                            Bucket: bucket,
                                            Key: key
                                        };
                                        console.log(deleteParams);
                                        s3.deleteObject(deleteParams, function(err, data) {
                                            if (err) console.log(err, err.stack); // an error occurred
                                            else {
                                                console.log(data);           // successful response
                                                context.succeed();
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    } else {
                        // otherwise, copy the file to inprogress bucket then rename it in indigestion
                        console.log("new file, so copy.");
                        var copyParams = {
                            Bucket: 'inprogress',
                            CopySource: 'indigestion' + '/' + key,
                            Key: key
                        };
                        console.log(copyParams);
                        s3.copyObject(copyParams, function(err, data) {
                            if (err) console.log(err, err.stack); // an error occurred
                            else {
                                console.log(data);           // successful response
                                copyParams = {
                                    Bucket: 'indigestion',
                                    CopySource:  'indigestion' + '/' + key,
                                    Key: 'accepted-' + key
                                };
                                s3.copyObject(copyParams, function(err, data) {
                                    if (err) console.log(err, err.stack); // an error occurred
                                    else {
                                        console.log(data);
                                        // now delete the file from indigestion
                                        var deleteParams = {
                                            Bucket: bucket,
                                            Key: key
                                        };
                                        console.log(deleteParams);
                                        s3.deleteObject(deleteParams, function(err, data) {
                                            if (err) console.log(err, err.stack); // an error occurred
                                            else {
                                                console.log(data);           // successful response
                                                context.succeed();
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    } else {
        context.succeed();
    }
};
