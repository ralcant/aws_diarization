import boto3
s3 = boto3.resource('s3')
'''
Uploads a video from the path {video_local_folder}/filename to the bucket_name bucket
'''
def upload_file(bucket_name, video_local_folder, filename):
    ### if the video just downloads then you can try opening on incognito ###
    filename_in = "videos/{}".format(filename)
    filename_out = "{}{}".format(video_local_folder, filename) 
    s3.Bucket(bucket_name).upload_file(filename_in, filename_out)
    print("just uploaded the file {} at {}".format(filename_in, filename_out))