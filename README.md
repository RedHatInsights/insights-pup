# Platform Upload Pre-Processor

The Platform Upload Pre-Processor (PUPP) is designed to recieve payloads from the Insights Upload
service via HTTP POST, extract facts from the payload, forward the information to the inventory
service, and finally respond to the upload service with the result.

## Details

The PUPP service is a component of the Insights Platform that validates the payloads uploaded
to Red Hat by clients. The service is engaged after an upload has been recieved and stored in
quarantine storage. It runs on Tornado 5 and Python 3.6.

PUPP retrieves payloads via the URL in the POST from the upload service, processes it through
the configured mechanism to extract needful information and guarantee integrity of the
archive, and send the extracted info to the inventory service. The success/fail response is then
returned to the upload service which responds to the originating client.

The service runs in Openshift Dedicated.

## How it Works

The PUPP service workflow is as follows:

  - The upload service sends a POST reflecting a new upload to be processed
  - The validator downloads the payload specified in the `url` key of the JSON
  - Insights Core is used to validate the tarfile and extract configurable facts from the archive
  - PUPP sends a POST to the inventory service containing the facts using the identity from the original principal
  - If Inventory post succeeds, validator responds to upload service with a 202 status

### JSON

The JSON expected by the PUPP service from the upload service looks like this:

```
{"rh_account": "123456",
 "principal": "test_org",
 "payload_id": "23oikjonsdv329",
 "size": 234,
 "service": "advisor",
 "url": "http://some.bucket.somewhere/1234"}
```

The JSON POSTed to the inventory service will be what is above, but also include facts:

```
...
"canonical_facts": {},
"fqdn": "some.host.name",
"insights-id": "asdf-we23-dcw-2342fdwc"}
```

**The above facts are configurable and may be added or taken away. The README should be updated to reflect those
changes**

Fields:

  - rh_account: The account number used to upload. Will be modified to `account_number` when posting to inventory
  - principal: The upload org_id. Will be modified to `org_id` when posting to inventory
  - payload_id: The ID of the individual uploaded archive
  - size: Size of the payload in bytes
  - service: The service name as provided by the MIME type. 
  - url: The url from which the archive will be downloaded
  - canonical_facts: A dictionary containing canonical_facts to be passed to inventory

Additional facts will also be included, but are configurable within the PUPP service. In the JSON above, `fqdn` and `insights-id` are examples of these additional facts.
  
The PUPP service will respond to the upload service with a success/failure JSON or an error code if something goes wrong.

Success example:

    {"validation": "success", "payload_id": "23oikjonsdv329"}

Failure example:

    {"validation": "failure", "payload_id": "23oikjonsdv329", "reason": "Inventory service failure"}

Available reasons for failure:

  - Inventory service failure
  - No facts could be retrieved
  - JSON keys must match ${REQUIRED KEYS}  
  
The "JSON keys must match.." failure results if `payload_id`, `url`, `rh_account`, or `principal` is missing.

### Errors

Any failures outside of the issues above will result in a 400 error sent back to the upload service. This will then need to be relayed back to the client

### Containers

A dockerfile is included with this repo for buiding the service using a container management tool (docker/buildah).

    buildah bud -t pupp:latest .

Or using docker:

    docker build -t pupp:latest .

#### Prerequisites

    buildah
    podman

Or using docker:

    docker

#### Running

The default environment variables should be acceptable for testing, though you may likely have to updat the `INVENTORY_URL` variable in order to point to your local inventory.

    podman run -d pupp:latest --name pupp
    OR
    docker run -d pupp:latest --name pupp

### Bare Metal

It's possible to run this app locally without engaging containers if you would like.

#### Prerequisites

    python3
    venv or pipenv

#### Python

Create a virtualenv using either venv or pipenv and install requirements. Once complete you can start the app by running `validator.py`

    virtualenv . -p $(which python3)
    source bin/activate
    pip install -r requirements.txt

Or using pipenv:

    pipenv install
    pipenv run ./validator.py

#### Running

Activate your virtual environment and run the validator

    source bin/activate
    python ./validator.py

Or with pipenv:

    pipenv run ./validator.py

#### Archive test location

One of the simplest ways to create a URL containing your example insights archive is using the nginx container.
Place an archive in a directory (`/home/user/something`) and then run an nginx server there. You may have to update your selinux context for the directory in order for it to mount properly. Substitute docker for podman if that's your jam.

    podman run -d -v /home/user/something:/usr/share/nginx/html:ro -p 8081:80 --name nginx nginx

## File Processing

This test assumes you have an inventory service available and ready to use. Visit the `insights-host-inventory` repo for those instructions. 

Use `curl` to post to the service and see if it all gets validated as expected. 

    curl -vvvv -d '{"rh_account": "12345", "principal": "12345", "url": "http://localhost:8081/some-archive.tar.gz", "payload_id": "123134kjlk"}' localhost:8080/api/validate

You should also be able to check the inventory service that a host was updated/added. Use a base64 encoded header for `x-rh-identity` that looks like this when decoded: `{"identity": {"account_number": "12345", "org_id": 12345"}}`

## Running with Tests

TODO: There is currently no PUPP test suite

## Deployment

The PUPP service `master` branch has a webhook that notifies Openshift Dedicated cluster to build a new image in the `buildfactory` project. The image build will then trigger a redployment of the service in `Platform-CI`. If the image is tested valid and operation, it should be tagged to `Platform-QA` for further testing, then finally copied to the Production cluster and the `Platform-Prod` project.

## Contributing

All outstanding issues or feature requests should be filed as Issues on this Github repo. PRs should be submitted against the master branch for any features or changes.

## Versioning

Anytime an endpoint is modified, the version should be incremented by `0.1`. New functionality that may effect other services should increment by `1`. Minor features and bugfixes can increment by `0.0.1`

## Authors

* **Stephen Adams** - **Initial Work** - [SteveHNH](https://github.com/SteveHNH)
* **Kyle Lape** - **Initial Work** - [kylape](https://github.com/kylape)
