import json
import requests
import zlib

index_file_url = 'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2023-04-01_anthem_index.json.gz'

# This is 1mb. From my experiment, it performs the best when the size is borderline bigger
# than the size of 1 element of 'reporting_plans'. If the chunk size is too small, it takes
# multiple chunks (decompressions) to be able to parse 1 reporting plan. If the chunk size
# is too big, it takes more memory space and likely carries a bigger buffer to the next chunk.
chunk_size = 1_048_576


# helper method to read from the index file in chunks
def read_remote_file_in_chunks(url, chunk_size):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size):
            yield chunk


line_count = 0
decompressor = zlib.decompressobj(wbits=zlib.MAX_WBITS | 16)
buffer = ''
for chunk in read_remote_file_in_chunks(index_file_url, chunk_size):
    decompressed_chunk = decompressor.decompress(chunk)
    lines = decompressed_chunk.decode('utf-8').split('\n')

    for line in lines:
        line_count += 1
        if line_count < 4:
            # We don't care the very first 3 lines in the json file and if we include them
            # it will never be a valid json until the end of the file.
            # This is what the first 3 lines look like.
            """
            {"reporting_entity_name":"Anthem Inc",
            "reporting_entity_type":"health insurance issuer",
            "reporting_structure":[
            """
            continue

        buffer += line.strip()

        try:
            # Need to remove the last character as it is a comma ',' after the closing curly '}'
            # to make a valid json object. This might fail on the very last reporting plans, but
            # it should be fine for this exercise.
            plans = json.loads(buffer[0:-1])

            # Find the reporting plan that has 'in_network_files' property and 'PPO' in the plan name.
            if 'in_network_files' in plans and any('PPO' in plan['plan_name'] for plan in plans['reporting_plans']):
                for file in plans['in_network_files']:
                    # Find the plan id whose files have 'New York' and 'PPO' in the description.
                    if 'New York' in file['description'] and 'PPO' in file['description']:
                        # Didn't bother the lenght check and null check :)
                        plan_id = plans['reporting_plans'][0]['plan_id']

                        # Get the MRFs from Anthem's EIN lookup.
                        files = requests.get(
                            f'https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/{plan_id}.json')
                        files_json = files.json()
                        in_network_files = files_json['In-Network Negotiated Rates Files']
                        # I feel like I don't need out of network files, but it didn't hurt to keep them here.
                        out_of_network_files = files_json['Out-of-Network Allowed Amounts Files']
                        bcbs_files = files_json['Blue Cross Blue Shield Association Out-of-Area Rates Files']
                        all_files = in_network_files + out_of_network_files + bcbs_files

                        # Get the urls that have 'NY_PPO' in the display name.
                        urls = [f['url']
                                for f in all_files if 'NY_PPO' in f['displayname']]

                        # Write the result to a file in bulk as the result set is small.
                        with open('urls.json', 'w') as f:
                            json.dump(urls, f, indent=2)

                        # Exit the program as we have found the result.
                        exit(0)

            buffer = ''
        except ValueError as err:
            # It is not a valid json yet. Keep adding new lines to the buffer.
            pass
