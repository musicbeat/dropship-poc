console.log('Loading function');
var aws = require('aws-sdk');
var s3 = new aws.S3();

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    // List the bucket contents
    var bucket = event.Records[0].s3.bucket.name;
    var key = event.Records[0].s3.object.key;
    console.log(bucket, '/', key);
    var getBucketAclParams = {
        Bucket: bucket /* required */
    };
    console.log("a");
    s3.getBucketAcl(getBucketAclParams, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
    });
    console.log("b");
    var getParams = {
        Bucket: bucket,
        Key: key
    };
    console.log(getParams);
    s3.getObject(getParams, function(err, data) {
        console.log("killroy");
        if (err) {
            console.log(err);
            context.fail(err);
        } else {
            console.log("ContentType", data.ContentType);
        }
    });
    s3.listObjects({Bucket: bucket}, function(err, listData) {
        console.log(listData);
        if (err) {
            console.log(err);
            context.fail(err);
        }
        for(var file in listData.Contents) {
            console.log("File: ", listData.Contents[file].Key);
        }
    });
    console.log("killroy again.");
    context.succeed();
};
