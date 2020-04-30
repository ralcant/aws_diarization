from __future__ import print_function
import json
import time
import boto3
import pprint
import datetime
import os
from bucket_handler import upload_file
import requests
transcribe = boto3.client('transcribe')
s3 = boto3.resource('s3')
transcribe = boto3.client('transcribe')
s3 = boto3.resource('s3')
## Note to self: in order to get the print statement as they happen in the command line, do "python -u main.py"
class Project:
    def __init__(self, bucket_name="prgdiarization", video_folder_name="videos/"):
        super().__init__()
        self.video_to_transcription = dict() # maps the video -> MediaFileUri of the transcription
        self.bucket_name = bucket_name       # bucket to be used for all the transactions
        self.transciption_job_name = "prg_diarization_test"
        self.video_folder_name = video_folder_name  
    '''
    Main method of the project class. It updates the output.json file with the results of the transcription for such video.
    '''
    def get_transcription(self, video):
        print('started transcription!')
        video_name = self.get_video_name(video)
        # t = datetime.datetime.now()
        time_transcription_started = time.time() #to calculate how much time it takes to do the job
        job_uri = "https://{}.s3-us-west-1.amazonaws.com/{}{}".format(self.bucket_name, self.video_folder_name, video)

        if (video_name not in self.video_to_transcription.keys()):             
            self.start_job(job_uri)
            while True:
                status = transcribe.get_transcription_job(TranscriptionJobName=self.transciption_job_name)
                if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                    break
                print("Not ready yet...")
                time.sleep(5)
            print('already got response and it took {} seconds!'.format(time.time() - time_transcription_started))
            response = status 
            MediaFileURI = response["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
            self.video_to_transcription[video_name] = MediaFileURI  #for future reference
            #self.delete_job()
        else:
            print("it already exists!")
            MediaFileURI = self.video_to_transcription[video_name] #'https://s3.us-west-1.amazonaws.com/aws-transcribe-us-west-1-prod/115847983408/prg_diarization_test_9/946d2d28-827b-49b7-9136-55e17b1b6eb6/asrOutput.json?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEHYaCXVzLXdlc3QtMSJHMEUCIGeiQRZoD22XmzdRasfvXOEbpIGcKYFKSGJzaSlfHjUiAiEA9CRg8SIHAskooNPVB%2BurejmIvAiKldYTO9h5Pb6y3A0qtAMIbxABGgw5NzEzODk5ODIxNjgiDCzAQQqVwGhTjzYcsyqRA%2FF8yOgyiYMepEqlOR7Wel%2BLlP2Ol32C5pljhoCALRUQjpnlclBDpzQqAkcdEhSNlF3xehp0bB5QTwi0BwLF1tizzN5F8kPLLEtM%2FK40b4yV1RhT3kzFlVv9rXFRMvUMPM4cfunr%2BUiLkC%2F4h8uyedEIo4RTq584u0R1JIYTsMHTm9XiInlruqw%2BS%2BOedWDlksOvI1z4PF6uhYrxo87Hy8Jjb3Ws7nEBPFP8mJexybX53RxgXJ7sNnoe7ichWG9JU3d31wJSNjnsNdtCdpikaZPGJC8oAXN0tE057tC9NUqYaag4wTaF9tX6llII45RXnCHVY0xG14Cq8mTXKdiMxFFgDjKvsITMNsHr6XNhgu%2BU%2B7s%2BFzdTHnpzPBrvRUi7JcDrJ%2Fdy3IILyJe3hHHsyIFqjjCrbQrXO%2Bwq6WV7%2FfLfh4%2BLGtKNuvd34b1u5wgWrzChpN2ha5sibJxBVcN3%2FqtghhvhZvuORwGQdFVrSkD%2BtbJNSSf72I2CuWsnsCctV8VAJl6rxHatD3%2Btcvlm9FpXMJjv6%2FMFOusBABniLSVZHh54XvoJ4NPAqE44fj%2BbKdABKV8%2BuUxbG97IGRuBVFcZjG%2B5GZYHFlpcFz1k3vNXixYaXdeznrOKalJBGRYXUSEfirQbty2s%2B4MHxdIhrFVR%2BofgMy6CE1L%2Bi%2BYdEJ%2FOWCprKT7ztYTLESgbLClMyUFkCPMRCQrMez9m5JTgucjc3As6mWlgREjxUfu3kHujqG592wydfbaegIRzqcYpAEcXPx2rsBorx9UJn%2BDk1lv97imTpqXVDXQOVDy%2FzEChpJBLPDaysQfBbf2gRKU70k%2Bj5%2BCNtfuavse5A7AJy53sAwusqg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20200325T070254Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=ASIA6EK222XMPPCNL4OQ%2F20200325%2Fus-west-1%2Fs3%2Faws4_request&X-Amz-Signature=3e6e2b818c4617412e3d094e46e3e05e03691845816f0e994c26da9cdf5a879d'
            #self.delete_job()
        r = requests.get(MediaFileURI)
        #response = r.json()
        response = json.loads(r.text) #getting the response from the given url
        self.delete_job() #we have the response, we can delete the job
        with open("outputs.json") as f:
            output = json.load(f)
            f.close()
        with open("outputs.json", "w") as json_output:
            #output= json.load(json_output)
            new_data = {
                video_name: {
                    "name": video,
                    "video_URI": job_uri,
                    #"transcription_URI": MediaFileURI,  #ignored because this is usually a very long name
                    "transcription": self.extract_useful_information(response),
                }
            } 
            output["items"].update(new_data)
            json.dump(output, json_output, indent=4)
            json_output.close()
        print("\n")
    '''
    Starts a transciption job for a certain video.
    TODO: Need to add more security constraints, or everything public is fine for now?
    '''
    def start_job(self, job_uri):
        transcribe.start_transcription_job(
            TranscriptionJobName = self.transciption_job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat='mp4',
            LanguageCode='en-US',
            Settings = { 
                "MaxSpeakerLabels": 2,
                "ShowSpeakerLabels": True,
            }
        )
    # Useful for when a transcription is suddenly stopped, and we are not allowed to do 
    # any other job because the name is already taken 
    def delete_job(self):
        transcribe.delete_transcription_job(
            TranscriptionJobName= self.transciption_job_name
        )
        print("deleted job with name {}".format(self.transciption_job_name))
    '''
    segments: a dictionarly that maps each speaker label to a list of the times that this speaker talked 
    Returns a mapping from each interval (start, end) to the speaker label that happened during that time.
    '''
    @staticmethod
    def get_interval_to_speaker_label(segments):
        interval_to_speaker = dict()
        for speaker_n in range(len(segments)):
            all_items = segments[speaker_n]["items"]
            for interval in all_items:
                start = interval["start_time"]
                end = interval["end_time"]
                speaker_label = interval["speaker_label"]
                interval_to_speaker[(start, end)] = speaker_label
        return interval_to_speaker
    '''
    Converts a response from AWS Transcribe into a (maybe) more readable version of it.
    It returns a dictionary with the following keys, values:
        - "num_speakers" -> The number of speaker determined by AWS. Defaults to "undetermined" if none were found.
        - "transcripts" -> The complete transcript of the video.
        - "items" -> a list of dictionaries, where each item represents one word said, 
                     and it has info about the start_time, end_time, speaker_label and the text used for such word.
    '''
    def extract_useful_information(self, response_dict):
        transcripts = response_dict["results"]["transcripts"]
        items = response_dict["results"]["items"]
        if  "speaker_labels" not in response_dict["results"]: # when AWS doesnt detect speakers. This *usually* means 
            return {                                          # that there is no transcript detected either.
                "num_speakers": "undetermined",               
                "transcripts": transcripts,
                "items": items,
            }
        speaker_labels = response_dict["results"]["speaker_labels"]
        num_speakers = speaker_labels["speakers"]
        interval_to_label = self.get_interval_to_speaker_label(speaker_labels["segments"])
        timestamps_with_speaker_labels = []
        for item in items:
            ## item is a dict for a particular timestamp
            if (item['type'] == "pronunciation"): #ignoring punctuation
                start = item["start_time"]
                end = item["end_time"]
                try:
                    speaker_label = interval_to_label[(start, end)]
                    item_copy = item.copy()
                    item_copy["speaker_label"] = speaker_label #just adding this new key of the "item" dictionary
                    timestamps_with_speaker_labels.append(item_copy)
                except:
                    print("Didnt find {} as a valid timestamp!".format((start, end)))
        ## now timestamps_with_speaker_labels is a list of all the relevant info ###
        #TODO: sort them by the (start, end)
        return {
            "num_speakers": num_speakers,
            "transcript": transcripts,
            "items": timestamps_with_speaker_labels
        }
    '''
    Extracts the name of a video. "example.mp4" --> "example"
    '''
    def get_video_name(self, video):
        return video.split(".")[0]
    '''
    Updates a certain video to our designed bucket. The video can be found in the directory video_folder_name/filename 
    Filename includes the .mp4 extension.
    '''
    def upload_video(self, filename):        
        upload_file(self.bucket_name , self.video_folder_name, filename)
    '''
    Helper method to update the outputs.json file with the videos found in  self.video_folder_name
    If lazy_update = True, then it will only update the file with the elements that dont already exist. If not, then 
    it will update the complete json file with all videos available. Assumes that all videos are mp4 files.
    '''
    def update_output_json(self, lazy_update = False): #by default this is gonna do everything again
        with open("outputs.json") as f:
            output = json.load(f)
            f.close()
        for filename in os.listdir(self.video_folder_name):
            if (lazy_update and self.get_video_name(filename) in output["items"].keys()):
                print("lazy_update is True and the filename {} already exists in output.json, so we ignore this file.".format(filename))
            else:
                if filename.endswith(".mp4"):  #if a video
                    print("uploading file {}...".format(filename))
                    upload_file(self.bucket_name , self.video_folder_name, filename) #uploading file to our bucket
                    self.get_transcription(filename) #updates output.json
                    print("done updating the json of the video: {}\n".format(filename))
                else:
                    print("not an .mp4 file! : {}. Ignoring :/ \n".format(filename))

if __name__ == "__main__":
    p = Project()
    ### updating the json file #######
    p.update_output_json(lazy_update=True)
    #p.upload_video("clip-42.mp4")
    #p.get_transcription("clip-54.mp4")
    # with open("outputs.json", "r") as transcript:
    #    pprint.pprint(json.load(transcript))