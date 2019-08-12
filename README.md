# Platform Upload Pre-Processor

The Platform Upload Pre-Processor (PUP) is designed to recieve payloads for the `advisor` service
via the message queue, extract facts from the payload, forward the information to the inventory
service, and finally respond to the upload service with the result.

## Details

The PUP service is a component of the Insights Platform that validates the payloads uploaded
to Red Hat by the insights-client. The service is engaged after an upload has been recieved and stored in
quarantine storage.

PUP retrieves payloads via URL in the message, processes it through
insights-core to extract facts and guarantee integrity of the archive, and send the extracted info to the inventory service. 
The success/fail response is then returned to the upload service to indicate whether to keep or reject the archive.

The service runs in Openshift Dedicated.

## How it Works

![UML](http://www.plantuml.com/plantuml/png/VLHHJzim47xFhpXbhr31O3nK4q8JG-nXKpVO4qMHwrnQRuwT-IvbNPN_FfyJMZO8aI39Tv-_-_nTt0jY5RYjRSQ6BSBi-mncmMiaCcu9LzO_PEXYRGEDLLpvK4_QndfLJbIvDkJ3M7cjh2aL6--oHWKssZJACJHjysm5ROYnReAlZCL_sAk5oKRw7fv7gIdKjCo-U4O8PhbYyDMOpobiyu2_MYGkJ9bF8LSBNHw_FJdD3-OkLrhxrh7WHt8T9xG9neaLAxXmtvqbdCmV54ZkXN75OiD8VGN3skXKB97Ok1G1372YQCMuz64Z7Danx02fjY4rMxJw9t8XRu8kpawZwpwuL5Q4Jw22E3fT9cgDh4h5AjAstTnrMSnf2ElIFpZ9X405lJ9hX2huUkvkJg3lzuJxyJS7fPcWIl-5-BXgxXuChy0uaipfKFk0cR8Cr6hvSZA8LrAAyvl2p1C3-yXVe-EuJrWR1TUtjxD4_6zwRJEik5Iifd2tpPC1sSt-gUlkqzgeFzZzXjHuHzWRlVxmQQzjiH9f8Uzv8iUUqsIF9JOCmGSHFvAt563gJgoutU4hJnoVtykz-47UGOeY6JlO9HBdSW3oXwmdvzrzDsBc1b6iWchCX1hKKFa0jGy89R8ozacIBnyMaN0bz1WCuFEiFtyTItgRzsTXnu_Mny_3QmUezr0C7leXPMXb8ihODk3ZNkxUYDxLrRriQkmt8ntivwdrdb-piPUGqj0wZqvScRsSuV2LIh-Bo6fh-nS0 "PUP Processing Flow")

The PUP service workflow is as follows:

  - Recieve a message from `platform.upload.advisor` topic in the MQ
  - PUP downloads the archive from the url specified in the message
  - Insights Core is engaged to open the tar file and extract the facts as determined by the `get_canonical_facts` method in insights-core
  - PUP sends a post to inventory containing those facts using the identity of the original uploader
  - If inventory post succeeds, the host ID is added to the return validation message
  - The validation message is sent back to the upload service via the `platform.upload.validation` queue

### JSON

The JSON expected by the PUP service from the upload service looks like this:

```
{"account": "123456",
 "principal": "test_org",
 "request_id": "23oikjonsdv329",
 "size": 234,
 "service": "advisor",
 "b64_identity": "<some big base64 string>",
 "metadata": "{'some_key': 'some_value'}, # optional
 "url": "http://some.bucket.somewhere/1234"}
```

The JSON POSTed to the inventory service will be what is above, but also include facts:

```
...
"facts": [{'facts': {"insights_id": "a756b571-e227-46a5-8bcc-3a567b7edfb1",
                    "machine_id": null,
                    "bios_uuid": null,
                    "subscription_manager_id": null,
                    "ip_addresses": [],
                    "mac_addresses": [],
                    "fqdn": "Z0JTXJ7YSG.test"}
            'namespace': 'insights-client'}]
```

**The above facts are managed by the [insights-core](https://www.github.com/RedHatInsights/insights-core) project and may be added or taken away. The README should be updated to reflect those
changes**

Fields:

  - account: The account number used to upload. Will be modified to `account_number` when posting to inventory
  - principal: The upload org_id. Will be modified to `org_id` when posting to inventory
  - request_id: The ID of the individual uploaded archive
  - size: Size of the payload in bytes
  - service: The service name as provided by the MIME type. 
  - url: The url from which the archive will be downloaded
  - facts: An array containing facts for each host
  
The PUP service will respond to the upload service with a message on the message queue regarding success or failure.

Success example:

    {"validation": "success", "request_id": "23oikjonsdv329"}

Failure example:

    {"validation": "failure", "request_id": "23oikjonsdv329"}

### Containers

A dockerfile is included with this repo for buiding the service using a container management tool (docker/buildah).

    buildah bud -t pup:latest .

Or using docker:

    docker build -t pup:latest .

#### Prerequisites

    buildah
    podman

Or using docker:

    docker

#### Running

The default environment variables should be acceptable for testing, though you may likely have to updat the `INVENTORY_URL` variable in order to point to your local inventory.

    podman run -d --name pup -p 8080:8080 pup:latest
    OR
    docker run -d --name pup -p 8080:8080 pup:latest

### Bare Metal

It's possible to run this app locally without engaging containers if you would like.

#### Prerequisites

    python3
    venv or pipenv

#### Python

Create a virtualenv using either venv or pipenv and install requirements. Once complete you can start the app by running `pup.py`

    virtualenv . -p $(which python3)
    source bin/activate
    pip install -r requirements.txt

Or using pipenv:

    pipenv install
    pipenv run ./pup.py

#### Running

Activate your virtual environment and run the validator

    source bin/activate
    python ./pup.py

Or with pipenv:

    pipenv run ./pup.py

## File Processing

The best way to test is by standing up this server and incorporating it with the upload-service. The [insights-upload](https://www.github.com/RedHatInsights/insights-upload) repo has a docker-compose that will get you most of the way there. Other details regarding
posting your archive to the service can be found in that readme.

This test assumes you have an inventory service available and ready to use. Visit the `insights-host-inventory` repo for those instructions. 

## Running with Tests

TODO: There is currently no PUP test suite

## Deployment

The PUP service `master` branch has a webhook that notifies Openshift Dedicated cluster to build a new image in the `buildfactory` project. The image build will then trigger a redployment of the service in `Platform-CI`. If the image is tested valid and operation, it should be tagged to `Platform-QA` for further testing, then finally copied to the Production cluster and the `Platform-Prod` project.

The commands for that process are as follows:

    ===In insights-dev cluster===
    oc tag platform-ci/insights-validator:latest platform-qa/insights-validator:latest
    
    ===Copy to production cluster===
    skopeo copy --src-creds=user:dev_login_token --dest-creds=user:prod_login_token \
    docker://registry.insights-dev.openshift.com/platform-qa/insights-validator:latest \
    docker://registry.insights.openshift.com/platform-stage/insights-validator:latest

    ===In insights production cluster===
    oc tag platform-stage/insights-validator:latest platform-prod/insights-validator:latest

## Contributing

All outstanding issues or feature requests should be filed as Issues on this Github repo. PRs should be submitted against the master branch for any features or changes.

## Running unit tests

To run unit tests, run `nosetests --with-coverage --cover-package=pup`.

## Versioning

Anytime an endpoint is modified, the version should be incremented by `0.1`. New functionality that may effect other services should increment by `1`. Minor features and bugfixes can increment by `0.0.1`

## Authors

* **Stephen Adams** - **Initial Work** - [SteveHNH](https://github.com/SteveHNH)
* **Kyle Lape** - **Initial Work** - [kylape](https://github.com/kylape)
