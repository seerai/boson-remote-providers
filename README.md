# Boson Remote Providers

This repo contains all current remote providers. 


# Remote Provider Template Walkthrough
This repo is meant to help jump start the creation of a remote provider that
wraps some external API serving feature data. While this application of a remote provider is
fairly straightforward, there are many options to handle, and each API presents its own
unique challenges. The main challenges in writing the provider are 1) translating a Dataset search
request in Geodesic to an API request for the provider, and 2) receiving the response and parsing the
results into Geodesic Features. This template is set up so that the user only has to provide the
details of those operations, depending on the API in question.

---

## Prototyping the Remote Provider

The first step in building a remote provider is familiarizing yourself with how the external API works. Some items you
will want to consider are:

- Is this a data source for which a boson provider already exists?
- Is the data spatiotemporal in nature?
- What are the terms of service of the API?
- Do you need to request an API key or other authorization to access the data?
- What are the parameters that the external API accepts?
- What does the data returned from the external API look like?

Once you've decided to write a remote provider, it is recommended that you investigate those last two questions
with a combination of whatever web interface the remote API provides (if any), and a Jupyter notebook. There is a sample
notebook in the `notebooks/` directory that you can use to get started.

## Writing the remote provider

The code for the remote provider is contained in `boson/provider.py`, which does the heavy lifting of translating a boson query
to an API request, and then translating the API response into a Geodesic `FeatureCollection`, which is what Boson needs to turn
the external API into a Boson `Dataset`. The `provider.py` in this repo is a template with sample code for each of the parameters that
need to be translated. Refer to the comments within for further direction on adapting the template to your particular use case.

## Building the Image

Once the provider is written, you need to create a Docker image to package the remote provider and associated data and packages.
Boson remote providers are built as docker images using the Boson Python SDK.
To build the image, change to the boson directory run the build script

```bash
sh build.sh 1
```

The number argument is the version number you would like to tag the image with.

## Running the Image Locally

If you want to test the image locally you can run it using the run script

```bash
sh run.sh 1
```

Here the number argument is the version number of the image you would like to run.
This will start the uvicorn server on port 8000.

There is also a test script that will call the search endpoint and print the results to make
sure that you are revieving the expected data.

```bash
python test.py
```

## Setting up the Remote Provider

To set up the remote provider, you will need to push the image to a container registry. In this case
we will push it to GCP artifact registry.

```bash
docker push us-central1-docker.pkg.dev/double-catfish-291717/seerai-docker/images/<image name>:v0.0.1
```

You then need to create a cloud run service using this image. On the GCP console go to cloud run and create a new service.
Select the image from the artifact registry and set the port to 8000. You can also set the memory and cpu limits here.
One problem I have found is that the cloud run service will overwrite some environment variables that may cause the container to fail. In the case of this particular image you will need to set the `HOME`
environment variable to `/root`. All of these settings are found on the creation page for the cloud run service.

Once the service is created you need to create the dataset in Entanglement that points to it. This is easy to do with the _from_ method:

```python
ds = geodesic.Dataset.from_remote_provider(
    name=<dataset-name>,
    url='<url-from-cloud-run>',
)
ds.save()
```

The cloud run URL can be found on the cloud run service page. Once the dataset is created you can
create a share link for it.

```python
share = ds.share_as_arcgis_service()
share.get_feature_layer_url()
```

With this URL you can add the layer into an Arcgis map.

## More info

You can see more information about remote providers, along with some step by step guidance in the [slides from our Intro to Remote Providers
from the Weekly Interesting Talk of Interest from 6/7/2024](https://seerai-my.sharepoint.com/:p:/g/personal/sgilhool_seerai_onmicrosoft_com/ETd3crhrRbtOoS_9W9ciNhoBeejufLu4QZ-i-hdEN2MW8A?e=HpORpB)
