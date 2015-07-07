function copyThenDelete(fromBucket, fromKey, toBucket, toKey) {
    var result = false;
    var copyParams = {
        Bucket: toBucket,
        CopySource: fromBucket + '/' + fromKey,
        Key: toKey
    };
    s3.copyObject(copyParams, function(err, data) {
        if (err) {
            console.log(err, err.stack); // an error occurred
            result = false;
        } else {
            console.log("copied to " + toBucket + "/" + toKey);  // successful response
            // now delete the file from indigestion
            var deleteParams = {
                Bucket: bucket,
                Key: key
            };
            s3.deleteObject(deleteParams, function(err, data) {
                if (err) {
                    console.log(err, err.stack); // an error occurred
                    result = false;
                } else {
                    console.log("deleted from " + fromBucket + "/" + fromKey);
                    result = true;
                }
            });
        }
    });
    return result;
}
