console.log('Loading function');
var aws = require('aws-sdk');
var s3 = new aws.S3({apiVersion: '2006-03-01'});
var sns = new aws.SNS();

exports.handler = function(event, context) {
    console.log("Received event ", JSON.stringify(event, null, 2));

    // Do a HEAD request on the object from the event and get its ETag (MD5)
    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    s3.headObject({Bucket: bucket, Key: key}, function(err, headData) {
        if (err) {
            console.log("Error getting object " + key + " from bucket " + bucket +
            ". Make sure they exist and the bucket is in the same region as this function.");
            context.fail("Error getting file: " + err);
        } else {
            var eTag = headData.ETag;
            // Look at all the other objects in the S3 bucket
            s3.listObjects({Bucket: bucket}, function(err, listData) {
                var match;
                for (var file in listData.Contents) {
                    // Check if the file checksums match
                    if ((eTag == listData.Contents[file].ETag) && (key != listData.Contents[file].Key)) {
                            match = file;
                            break;
                        }
                }
                // If there was a duplicate, then publish to SNS
                if (match) {
                    var payload = "Uploaded file " + key + " matches existing file " + listData.Contents[file].Key;
                    var params = {
                        Message: payload,
                        Subject: 'Duplicate file received',
                        TopicArn: 'arn:aws:sns:us-west-2:134529390395:dropship-poc-alarm'
                    };
                    sns.publish(params, function(err, data) {
                        if (err) {
                            console.log("Error sending SNS ", err);
                        }
                        context.succeed();
                    });
                    context.fail("Duplicate file received: " + err);
                }
            });
        }
    });
    // Continue by copying the s3 object in order to rename it
    var params = {
        Bucket: bucket, /* source and destination are the same */
        CopySource: bucket + '/' + key,
        Key: 'ready/' + key,
    };
    s3.copyObject(params, function(err, data) {
        if (err) {
            console.log(err, err.stack);
            context.fail("Error copying file" + err);
        }
    });
};
