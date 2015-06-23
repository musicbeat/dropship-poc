console.log('Loading function');
var aws = require('aws-sdk');
var s3 = new aws.S3();
var sns = new aws.SNS();

exports.handler = function(event, context) {
    console.log("Received event ", JSON.stringify(event, null, 2));

    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;

    // Parameterize, perhaps with metadata on the event's bucket.
    var destBucket = "ready-digestion";

    console.log("get md5 of event's object.");

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
                var match;
                for (var file in listData.Contents) {
                    console.log("Existing file: ", listData.Contents[file].Key);
                    // Check if the file checksums match
                    if ((eTag == listData.Contents[file].ETag) && (key != listData.Contents[file].Key)) {
                        match = file;
                        break;
                    }
                }
                // If there was a duplicate, then publish to SNS
                if (match) {
                    var payload = "Uploaded file " + key + " matches existing file " + listData.Contents[file].Key;
                    var snsParams = {
                        Message: payload,
                        Subject: 'Duplicate file received',
                        TopicArn: 'arn:aws:sns:us-west-2:134529390395:dropship-poc-alarm'
                    };
                    sns.publish(snsParams, function(err, data) {
                        if (err) {
                            console.log("Error sending SNS ", err);
                        }
                    });
                    console.log("Duplicate file received: " + listData.Contents[file].Key);
                } else {
                    // Continue by copying the s3 object in order to rename it
                    var copyParams = {
                        Bucket: destBucket,
                        Key: key,
                        CopySource: bucket + '/' + key
                    };
                    s3.copyObject(copyParams, function(err, data) {
                        if (err) {
                            console.log(err, err.stack);
                            context.fail("Error copying file" + err);
                        }
                    });
                    console.log("File copied: ", copyParams);
                }
            });
        }
    });
    console.log("success.");
    context.succeed();
};
